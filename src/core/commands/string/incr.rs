// src/core/commands/string/incr.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Shared logic for INCR, DECR, and INCRBY commands.
pub async fn do_incr_decr_by(
    key: &Bytes,
    by: i64,
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

    // Check if the key exists and handle its current value.
    if let Some(entry) = shard_cache_guard.get_mut(key) {
        if entry.is_expired() {
            // Expired key is treated as non-existent; it will be overwritten.
            shard_cache_guard.pop(key);
        } else if let DataValue::String(s) = &mut entry.data {
            let current_val: i64 = std::str::from_utf8(s)?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?;

            let new_val = current_val.checked_add(by).ok_or(SpinelDBError::Overflow)?;

            let old_size = s.len();
            *s = Bytes::from(new_val.to_string());
            let new_size = s.len();

            let mem_diff = new_size as isize - old_size as isize;
            // Safely update the entry size using saturating arithmetic.
            if mem_diff > 0 {
                entry.size = entry.size.saturating_add(mem_diff as usize);
            } else {
                entry.size = entry.size.saturating_sub((-mem_diff) as usize);
            }
            entry.version = entry.version.wrapping_add(1);
            shard.update_memory(mem_diff);

            return Ok((
                RespValue::Integer(new_val),
                WriteOutcome::Write { keys_modified: 1 },
            ));
        } else {
            return Err(SpinelDBError::WrongType);
        }
    }

    // Key does not exist, create it with the increment value.
    let new_val_bytes = Bytes::from(by.to_string());
    let new_stored_value = StoredValue::new(DataValue::String(new_val_bytes));
    shard_cache_guard.put(key.clone(), new_stored_value);

    Ok((
        RespValue::Integer(by),
        WriteOutcome::Write { keys_modified: 1 },
    ))
}

/// Represents the `INCR` command.
#[derive(Debug, Clone, Default)]
pub struct Incr {
    pub key: Bytes,
}

impl ParseCommand for Incr {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "INCR")?;
        Ok(Incr {
            key: extract_bytes(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Incr {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        do_incr_decr_by(&self.key, 1, ctx).await
    }
}

impl CommandSpec for Incr {
    fn name(&self) -> &'static str {
        "incr"
    }

    fn arity(&self) -> i64 {
        2
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
        vec![self.key.clone()]
    }
}
