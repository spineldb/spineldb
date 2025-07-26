// src/core/commands/zset/zrem.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct ZRem {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}
impl ParseCommand for ZRem {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, members) = parse_key_and_values(args, 2, "ZREM")?;
        Ok(ZRem { key, members })
    }
}
#[async_trait]
impl ExecutableCommand for ZRem {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }
        if let DataValue::SortedSet(zset) = &mut entry.data {
            let old_mem = zset.memory_usage();
            let removed_count = self.members.iter().filter(|m| zset.remove(m)).count();
            let outcome = if removed_count > 0 {
                let new_mem = zset.memory_usage();
                entry.version = entry.version.wrapping_add(1);
                entry.size = new_mem;
                shard
                    .current_memory
                    .fetch_sub(old_mem - new_mem, Ordering::Relaxed);
                WriteOutcome::Write { keys_modified: 1 }
            } else {
                WriteOutcome::DidNotWrite
            };
            if zset.is_empty() {
                shard_cache_guard.pop(&self.key);
            }
            Ok((RespValue::Integer(removed_count as i64), outcome))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}
impl CommandSpec for ZRem {
    fn name(&self) -> &'static str {
        "zrem"
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
        args.extend(self.members.clone());
        args
    }
}
