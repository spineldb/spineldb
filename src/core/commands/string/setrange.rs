// src/core/commands/string/setrange.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

/// A hard limit on memory allocation for a single SETRANGE operation to prevent DoS.
const MAX_SETRANGE_ALLOCATION: usize = 512 * 1024 * 1024; // 512 MB, same as Redis string limit

/// Represents the `SETRANGE` command.
#[derive(Debug, Clone, Default)]
pub struct SetRange {
    pub key: Bytes,
    pub offset: usize,
    pub value: Bytes,
}

impl ParseCommand for SetRange {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "SETRANGE")?;
        Ok(SetRange {
            key: extract_bytes(&args[0])?,
            offset: extract_string(&args[1])?.parse()?,
            value: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for SetRange {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Calculate the required final length of the string after the operation.
        let required_len = self.offset.saturating_add(self.value.len());

        // Pre-flight safety checks before locking.
        if required_len > MAX_SETRANGE_ALLOCATION {
            return Err(SpinelDBError::InvalidState(
                "string length is greater than maximum allowed size (512MB)".to_string(),
            ));
        }

        if let Some(maxmem) = ctx.state.config.lock().await.maxmemory {
            let total_memory: usize = ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();
            let old_len = if let Ok(guard) = ctx
                .db
                .get_shard(ctx.db.get_shard_index(&self.key))
                .entries
                .try_lock()
            {
                guard.peek(&self.key).map_or(0, |e| e.size)
            } else {
                0
            };
            let estimated_increase = required_len.saturating_sub(old_len);
            if total_memory.saturating_add(estimated_increase) > maxmem
                && !ctx.db.evict_one_key(&ctx.state).await
            {
                return Err(SpinelDBError::MaxMemoryReached);
            }
        }

        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        let entry = guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::String(Bytes::new()))
        });

        // Treat an expired key as a new, empty key.
        if entry.is_expired() {
            entry.data = DataValue::String(Bytes::new());
            entry.expiry = None;
        }

        if let DataValue::String(s) = &mut entry.data {
            let old_len = s.len();
            let mut new_bytes = BytesMut::from(s.as_ref());

            // Grow the string with null bytes if the offset is beyond the current length.
            if required_len > new_bytes.len() {
                new_bytes.resize(required_len, 0);
            }

            // Perform the overwrite operation.
            new_bytes[self.offset..required_len].copy_from_slice(&self.value);
            *s = new_bytes.freeze();

            let final_len = s.len();
            let mem_diff = final_len as isize - old_len as isize;

            // Update metadata only if the string's size actually changed.
            if mem_diff != 0 {
                if mem_diff > 0 {
                    entry.size = entry.size.saturating_add(mem_diff as usize);
                } else {
                    entry.size = entry.size.saturating_sub((-mem_diff) as usize);
                }
                entry.version += 1;
                shard.update_memory(mem_diff);
            }

            Ok((
                RespValue::Integer(final_len as i64),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for SetRange {
    fn name(&self) -> &'static str {
        "setrange"
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
            self.offset.to_string().into(),
            self.value.clone(),
        ]
    }
}
