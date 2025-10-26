// src/core/commands/string/bitop.rs

use crate::config::EvictionPolicy;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

/// Defines the supported bitwise operations.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum BitOpOperation {
    #[default]
    And,
    Or,
    Xor,
    Not,
}

/// Represents the BITOP command and its arguments.
#[derive(Debug, Clone, Default)]
pub struct BitOp {
    pub operation: BitOpOperation,
    pub dest_key: Bytes,
    pub src_keys: Vec<Bytes>,
}

impl ParseCommand for BitOp {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("BITOP".to_string()));
        }

        let op_str = extract_string(&args[0])?.to_ascii_uppercase();
        let operation = match op_str.as_str() {
            "AND" => BitOpOperation::And,
            "OR" => BitOpOperation::Or,
            "XOR" => BitOpOperation::Xor,
            "NOT" => BitOpOperation::Not,
            _ => return Err(SpinelDBError::SyntaxError),
        };

        let dest_key = extract_bytes(&args[1])?;
        let src_keys: Vec<Bytes> = args[2..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        // The NOT operation requires exactly one source key.
        if operation == BitOpOperation::Not && src_keys.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("BITOP NOT".to_string()));
        }
        if src_keys.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("BITOP".to_string()));
        }

        Ok(BitOp {
            operation,
            dest_key,
            src_keys,
        })
    }
}

#[async_trait]
impl ExecutableCommand for BitOp {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "BITOP requires multi-shard lock".into(),
                ));
            }
        };

        // Fetch all source operands first to determine required allocation size.
        let mut string_operands: Vec<Bytes> = Vec::with_capacity(self.src_keys.len());
        for key in &self.src_keys {
            let shard_index = ctx.db.get_shard_index(key);
            let guard = guards
                .get(&shard_index)
                .ok_or_else(|| SpinelDBError::Internal("Missing shard lock for BITOP".into()))?;

            let s = if let Some(entry) = guard.peek(key).filter(|e| !e.is_expired()) {
                if let DataValue::String(s_val) = &entry.data {
                    s_val.clone()
                } else {
                    return Err(SpinelDBError::WrongType);
                }
            } else {
                Bytes::new()
            };
            string_operands.push(s);
        }

        let max_len = string_operands.iter().map(|s| s.len()).max().unwrap_or(0);

        // --- Pre-flight check against maxmemory ---
        if let Some(maxmem) = ctx.state.config.lock().await.maxmemory {
            let dest_shard_index = ctx.db.get_shard_index(&self.dest_key);
            let old_dest_size = guards
                .get(&dest_shard_index)
                .and_then(|guard| guard.peek(&self.dest_key))
                .filter(|entry| !entry.is_expired())
                .map_or(0, |entry| entry.size);

            let estimated_increase = max_len.saturating_sub(old_dest_size);
            let total_memory: usize = ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();

            if total_memory.saturating_add(estimated_increase) > maxmem {
                let policy = ctx.state.config.lock().await.maxmemory_policy;
                if policy == EvictionPolicy::NoEviction {
                    return Err(SpinelDBError::MaxMemoryReached);
                }

                // Attempt to evict a key to make space.
                if !ctx.db.evict_one_key(&ctx.state).await {
                    // Eviction failed, so we are still over the limit.
                    return Err(SpinelDBError::MaxMemoryReached);
                }

                // Re-check memory after eviction.
                let total_memory_after_evict: usize =
                    ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();
                if total_memory_after_evict.saturating_add(estimated_increase) > maxmem {
                    return Err(SpinelDBError::MaxMemoryReached);
                }
            }
        }

        // Compute the result of the bitwise operation.
        let result_bytes = if self.operation == BitOpOperation::Not {
            // Logic for NOT operation.
            let src_string = &string_operands[0];
            let mut inverted = BytesMut::from(src_string.as_ref());
            for byte in inverted.iter_mut() {
                *byte = !*byte;
            }
            inverted.freeze()
        } else if max_len == 0 {
            Bytes::new()
        } else {
            // Logic for AND, OR, XOR operations.
            let initial_value = if self.operation == BitOpOperation::And {
                0xFF
            } else {
                0x00
            };
            let mut result = BytesMut::with_capacity(max_len);
            result.resize(max_len, initial_value);

            for operand in string_operands.iter() {
                for i in 0..max_len {
                    let other_byte = operand.get(i).copied().unwrap_or(0);
                    match self.operation {
                        BitOpOperation::And => result[i] &= other_byte,
                        BitOpOperation::Or => result[i] |= other_byte,
                        BitOpOperation::Xor => result[i] ^= other_byte,
                        BitOpOperation::Not => unreachable!(),
                    }
                }
            }
            result.freeze()
        };

        let result_len = result_bytes.len();
        let dest_shard_index = ctx.db.get_shard_index(&self.dest_key);
        let dest_guard = guards
            .get_mut(&dest_shard_index)
            .ok_or_else(|| SpinelDBError::Internal("Missing shard lock for BITOP dest".into()))?;

        // Store the result in the destination key.
        let new_value = StoredValue::new(DataValue::String(result_bytes));
        dest_guard.put(self.dest_key.clone(), new_value);

        Ok((
            RespValue::Integer(result_len as i64),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for BitOp {
    fn name(&self) -> &'static str {
        "bitop"
    }

    fn arity(&self) -> i64 {
        -4
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }

    fn first_key(&self) -> i64 {
        2 // First key is always the destination.
    }

    fn last_key(&self) -> i64 {
        -1 // All remaining args are source keys.
    }

    fn step(&self) -> i64 {
        1
    }

    fn get_keys(&self) -> Vec<Bytes> {
        let mut keys = Vec::with_capacity(self.src_keys.len() + 1);
        keys.push(self.dest_key.clone());
        keys.extend_from_slice(&self.src_keys);
        keys
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = Vec::with_capacity(self.src_keys.len() + 2);
        let op_str = match self.operation {
            BitOpOperation::And => "AND",
            BitOpOperation::Or => "OR",
            BitOpOperation::Xor => "XOR",
            BitOpOperation::Not => "NOT",
        };
        args.push(Bytes::from_static(op_str.as_bytes()));
        args.push(self.dest_key.clone());
        args.extend_from_slice(&self.src_keys);
        args
    }
}
