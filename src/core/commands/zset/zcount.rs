// src/core/commands/zset/zcount.rs

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

#[derive(Debug, Clone, Default)]
pub struct ZCount {
    pub key: Bytes,
    pub min: ScoreBoundary,
    pub max: ScoreBoundary,
}
impl ParseCommand for ZCount {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "ZCOUNT")?;
        Ok(ZCount {
            key: extract_bytes(&args[0])?,
            min: parse_score_boundary(&extract_string(&args[1])?)?,
            max: parse_score_boundary(&extract_string(&args[2])?)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ZCount {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Use consistent helper and handle passive expiration.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let count = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                0
            } else if let DataValue::SortedSet(zset) = &entry.data {
                zset.get_range_by_score(self.min.clone(), self.max.clone())
                    .len()
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            0
        };
        Ok((RespValue::Integer(count as i64), WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for ZCount {
    fn name(&self) -> &'static str {
        "zcount"
    }
    fn arity(&self) -> i64 {
        4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
