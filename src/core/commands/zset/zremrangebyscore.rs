// src/core/commands/zset/zremrangebyscore.rs

use super::helpers::parse_score_boundary;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::db::zset::ScoreBoundary;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct ZRemRangeByScore {
    pub key: Bytes,
    pub min: ScoreBoundary,
    pub max: ScoreBoundary,
}
impl ParseCommand for ZRemRangeByScore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "ZREMRANGEBYSCORE")?;
        Ok(ZRemRangeByScore {
            key: extract_bytes(&args[0])?,
            min: parse_score_boundary(&extract_string(&args[1])?)?,
            max: parse_score_boundary(&extract_string(&args[2])?)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ZRemRangeByScore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        if let DataValue::SortedSet(zset) = &mut entry.data {
            let old_mem = zset.memory_usage();
            let removed_count = zset.remove_range_by_score(self.min.clone(), self.max.clone());

            if removed_count == 0 {
                return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
            }

            let new_mem = zset.memory_usage();
            entry.size = new_mem;
            entry.version = entry.version.wrapping_add(1);
            shard
                .current_memory
                .fetch_sub(old_mem - new_mem, Ordering::Relaxed);

            let outcome = if zset.is_empty() {
                shard_cache_guard.pop(&self.key);
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::Write { keys_modified: 1 }
            };
            Ok((RespValue::Integer(removed_count as i64), outcome))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}
impl CommandSpec for ZRemRangeByScore {
    fn name(&self) -> &'static str {
        "zremrangebyscore"
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
            self.min.to_string().into(),
            self.max.to_string().into(),
        ]
    }
}
