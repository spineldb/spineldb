// src/core/commands/list/ltrim.rs

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

#[derive(Debug, Clone, Default)]
pub struct LTrim {
    pub key: Bytes,
    pub start: i64,
    pub stop: i64,
}

impl ParseCommand for LTrim {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "LTRIM")?;
        Ok(LTrim {
            key: extract_bytes(&args[0])?,
            start: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
            stop: extract_string(&args[2])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for LTrim {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        };

        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        let DataValue::List(list) = &mut entry.data else {
            return Err(SpinelDBError::WrongType);
        };

        if list.is_empty() {
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        let len = list.len() as i64;

        // Convert Redis-style indices (which can be negative) to 0-based usize indices.
        let start = if self.start >= 0 {
            self.start
        } else {
            len + self.start
        }
        .max(0) as usize;
        let stop = if self.stop >= 0 {
            self.stop
        } else {
            len + self.stop
        }
        .max(0) as usize;

        // If the start is after the end, or the start is beyond the list, the list becomes empty.
        if start > stop || start >= list.len() {
            shard_cache_guard.pop(&self.key);
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Delete { keys_deleted: 1 },
            ));
        }

        // The stop index is inclusive, so the end of the retained slice is `stop + 1`.
        let end_exclusive = (stop + 1).min(list.len());

        // Efficiently retain only the specified slice.
        let original_len = list.len();
        list.drain(end_exclusive..);
        list.drain(0..start);

        // If nothing was removed, it's a no-op.
        if list.len() == original_len {
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        let outcome = if list.is_empty() {
            shard_cache_guard.pop(&self.key);
            WriteOutcome::Delete { keys_deleted: 1 }
        } else {
            // The list was modified but is not empty. Recalculate size.
            let new_size = list.iter().map(|b| b.len()).sum();
            let old_size = entry.size;
            let mem_diff = new_size as isize - old_size as isize;

            entry.size = new_size;
            entry.version = entry.version.wrapping_add(1);
            shard.update_memory(mem_diff);
            WriteOutcome::Write { keys_modified: 1 }
        };

        Ok((RespValue::SimpleString("OK".into()), outcome))
    }
}

impl CommandSpec for LTrim {
    fn name(&self) -> &'static str {
        "ltrim"
    }
    fn arity(&self) -> i64 {
        4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
            self.start.to_string().into(),
            self.stop.to_string().into(),
        ]
    }
}
