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
use std::sync::atomic::Ordering;

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
        let start = if self.start >= 0 {
            self.start
        } else {
            len + self.start
        }
        .max(0);
        let stop = if self.stop >= 0 {
            self.stop
        } else {
            len + self.stop
        }
        .max(0);

        if start > stop || start >= len {
            // The range is empty or invalid, so the entire list should be deleted.
            shard_cache_guard.pop(&self.key);
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Delete { keys_deleted: 1 },
            ));
        }

        let start_usize = start as usize;
        let desired_len = (stop - start + 1) as usize;
        let mut mem_freed = 0;

        // Efficiently drain elements from the front.
        for val in list.drain(0..start_usize) {
            mem_freed += val.len();
        }

        // Efficiently truncate elements from the end.
        if list.len() > desired_len {
            for val in list.drain(desired_len..) {
                mem_freed += val.len();
            }
        }

        if mem_freed == 0 {
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        // After trimming, if the list is now empty, delete the key.
        if list.is_empty() {
            shard_cache_guard.pop(&self.key);
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Delete { keys_deleted: 1 },
            ));
        }

        // The list was modified but is not empty. Update metadata.
        entry.size -= mem_freed;
        entry.version = entry.version.wrapping_add(1);
        shard.current_memory.fetch_sub(mem_freed, Ordering::Relaxed);

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 1 },
        ))
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
