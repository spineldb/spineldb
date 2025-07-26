// src/core/commands/streams/xdel.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::stream::StreamId;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct XDel {
    key: Bytes,
    ids: BTreeSet<StreamId>,
}

impl ParseCommand for XDel {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("XDEL".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let ids = args[1..]
            .iter()
            .map(|frame| {
                extract_string(frame)?
                    .parse::<StreamId>()
                    .map_err(|e| SpinelDBError::InvalidState(e.to_string()))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;

        Ok(XDel { key, ids })
    }
}

#[async_trait]
impl ExecutableCommand for XDel {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, guard) = ctx.get_single_shard_context_mut()?;

        let entry = guard.get_mut(&self.key).ok_or(SpinelDBError::KeyNotFound)?;
        if entry.is_expired() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        if let DataValue::Stream(stream) = &mut entry.data {
            let old_mem = stream.memory_usage();
            let deleted_count = stream.delete(&self.ids);

            if deleted_count > 0 {
                let new_mem = stream.memory_usage();
                entry.size = new_mem;
                if old_mem > new_mem {
                    shard
                        .current_memory
                        .fetch_sub(old_mem - new_mem, Ordering::Relaxed);
                }
                entry.version += 1;
                Ok((
                    RespValue::Integer(deleted_count as i64),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for XDel {
    fn name(&self) -> &'static str {
        "xdel"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone()];
        args.extend(self.ids.iter().map(|id| id.to_string().into()));
        args
    }
}
