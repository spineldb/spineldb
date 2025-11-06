// src/core/commands/string/incrbyfloat.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use ryu;

/// Shared logic for INCRBYFLOAT.
pub async fn do_incr_decr_by_float(
    key: &Bytes,
    by: f64,
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

    if let Some(entry) = shard_cache_guard.get_mut(key) {
        if entry.is_expired() {
            shard_cache_guard.pop(key);
        } else if let DataValue::String(s) = &mut entry.data {
            let current_val: f64 = std::str::from_utf8(s)?
                .parse()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            let new_val = current_val + by;

            let mut buffer = ryu::Buffer::new();
            let formatted_new_val = buffer.format(new_val);
            let new_bytes = Bytes::copy_from_slice(formatted_new_val.as_bytes());

            let old_size = s.len();
            let new_size = new_bytes.len();
            *s = new_bytes;

            let mem_diff = new_size as isize - old_size as isize;
            entry.size = (entry.size as isize + mem_diff) as usize;
            entry.version = entry.version.wrapping_add(1);

            shard.update_memory(mem_diff);

            return Ok((
                RespValue::BulkString(Bytes::copy_from_slice(formatted_new_val.as_bytes())),
                WriteOutcome::Write { keys_modified: 1 },
            ));
        } else {
            return Err(SpinelDBError::WrongType);
        }
    }

    // Key does not exist, create it with the increment value.
    let mut buffer = ryu::Buffer::new();
    let formatted_val = buffer.format(by);
    let new_val_bytes = Bytes::copy_from_slice(formatted_val.as_bytes());

    let new_stored_value = StoredValue::new(DataValue::String(new_val_bytes));
    shard_cache_guard.put(key.clone(), new_stored_value);

    Ok((
        RespValue::BulkString(Bytes::copy_from_slice(formatted_val.as_bytes())),
        WriteOutcome::Write { keys_modified: 1 },
    ))
}

#[derive(Debug, Clone, Default)]
pub struct IncrByFloat {
    pub key: Bytes,
    pub increment: f64,
}
impl ParseCommand for IncrByFloat {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "INCRBYFLOAT")?;
        let increment = extract_string(&args[1])?
            .parse::<f64>()
            .map_err(|_| SpinelDBError::NotAFloat)?;
        Ok(IncrByFloat {
            key: extract_bytes(&args[0])?,
            increment,
        })
    }
}
#[async_trait]
impl ExecutableCommand for IncrByFloat {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        do_incr_decr_by_float(&self.key, self.increment, ctx).await
    }
}
impl CommandSpec for IncrByFloat {
    fn name(&self) -> &'static str {
        "incrbyfloat"
    }
    fn arity(&self) -> i64 {
        3
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
        let mut buffer = ryu::Buffer::new();
        let formatted_incr = buffer.format(self.increment);
        vec![
            self.key.clone(),
            Bytes::copy_from_slice(formatted_incr.as_bytes()),
        ]
    }
}
