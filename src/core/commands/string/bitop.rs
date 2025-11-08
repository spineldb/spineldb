// src/core/commands/string/bitop.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

/// Defines the supported bitwise operations for the BITOP command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BitOpOperation {
    #[default]
    And,
    Or,
    Xor,
    Not,
}

/// Represents the BITOP command and its parsed arguments.
#[derive(Debug, Clone, Default)]
pub struct BitOp {
    pub operation: BitOpOperation,
    pub dest_key: Bytes,
    pub src_keys: Vec<Bytes>,
}

impl ParseCommand for BitOp {
    /// Parses the command arguments from the RESP frame.
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
    /// Executes the BITOP command.
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

        // --- Phase 1: Pre-flight checks and operand fetching ---
        let mut string_operands: Vec<Bytes> = Vec::with_capacity(self.src_keys.len());
        for key in &self.src_keys {
            let shard_index = ctx.db.get_shard_index(key);
            let guard = guards.get(&shard_index).ok_or_else(|| {
                SpinelDBError::Internal("Missing shard lock for BITOP source".into())
            })?;

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

        // Safety check: Abort if the resulting string would exceed the configured limit.
        let max_alloc_size = ctx.state.config.lock().await.safety.max_bitop_alloc_size;
        if max_alloc_size > 0 && max_len > max_alloc_size {
            return Err(SpinelDBError::InvalidState(format!(
                "BITOP result would exceed 'max_bitop_alloc_size' limit ({} > {})",
                max_len, max_alloc_size
            )));
        }

        // --- Phase 2: Compute the result of the bitwise operation ---
        let result_bytes = if self.operation == BitOpOperation::Not {
            // Logic for NOT operation (single operand).
            let src_string = &string_operands[0];
            let mut inverted = BytesMut::from(src_string.as_ref());
            for byte in inverted.iter_mut() {
                *byte = !*byte;
            }
            inverted.freeze()
        } else if max_len == 0 {
            // If all operands are empty or non-existent, the result is an empty string.
            Bytes::new()
        } else {
            // Logic for AND, OR, XOR operations on multiple operands.
            let initial_value = if self.operation == BitOpOperation::And {
                0xFF
            } else {
                0x00
            };
            let mut result = BytesMut::with_capacity(max_len);
            result.resize(max_len, initial_value);

            for operand in string_operands.iter() {
                // Iterate up to the length of the current result buffer.
                for (i, res_byte) in result.iter_mut().enumerate() {
                    let other_byte = operand.get(i).copied().unwrap_or(0);
                    match self.operation {
                        BitOpOperation::And => *res_byte &= other_byte,
                        BitOpOperation::Or => *res_byte |= other_byte,
                        BitOpOperation::Xor => *res_byte ^= other_byte,
                        _ => unreachable!(),
                    }
                }
            }
            result.freeze()
        };

        let result_len = result_bytes.len();
        let dest_shard_index = ctx.db.get_shard_index(&self.dest_key);
        let dest_guard = guards.get_mut(&dest_shard_index).ok_or_else(|| {
            SpinelDBError::Internal("Missing shard lock for BITOP destination".into())
        })?;

        // --- Phase 3: Store the result ---
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
