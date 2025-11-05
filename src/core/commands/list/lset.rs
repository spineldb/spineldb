// src/core/commands/list/lset.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Represents the `LSET` command.
#[derive(Debug, Clone, Default)]
pub struct LSet {
    pub key: Bytes,
    pub index: i64,
    pub element: Bytes,
}

impl ParseCommand for LSet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "LSET")?;
        Ok(LSet {
            key: extract_bytes(&args[0])?,
            index: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
            element: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for LSet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Err(SpinelDBError::KeyNotFound);
        };

        if entry.is_expired() {
            // Expired key is treated as non-existent for this operation.
            shard_cache_guard.pop(&self.key);
            return Err(SpinelDBError::KeyNotFound);
        }

        if let DataValue::List(list) = &mut entry.data {
            let len = list.len() as i64;
            // Convert Redis-style index (can be negative) to a valid usize index.
            let index = if self.index >= 0 {
                self.index
            } else {
                len + self.index
            };

            if index < 0 || index >= len {
                return Err(SpinelDBError::InvalidState(
                    "index out of range".to_string(),
                ));
            }

            if let Some(old_element) = list.get_mut(index as usize) {
                let mem_diff = self.element.len() as isize - old_element.len() as isize;
                *old_element = self.element.clone();

                // Safely update the entry size using saturating arithmetic.
                if mem_diff > 0 {
                    entry.size = entry.size.saturating_add(mem_diff as usize);
                } else {
                    entry.size = entry.size.saturating_sub((-mem_diff) as usize);
                }

                entry.version = entry.version.wrapping_add(1);
                shard.update_memory(mem_diff);

                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                // This case should be unreachable due to the index check above.
                Err(SpinelDBError::Internal(
                    "LSET failed to get mutable element".into(),
                ))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for LSet {
    fn name(&self) -> &'static str {
        "lset"
    }

    fn arity(&self) -> i64 {
        4
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }

    fn first_key(&self) -> i64 {
        1
    }

    fn last_key(&self) -> i64 {
        1
    }

    fn step(&self) -> i64 {
        1
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            self.key.clone(),
            self.index.to_string().into(),
            self.element.clone(),
        ]
    }
}
