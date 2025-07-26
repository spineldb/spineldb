// src/core/commands/zset/zrevrange.rs

use super::helpers::{format_zrange_response, parse_range_args};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZRevRange {
    pub key: Bytes,
    pub start: i64,
    pub stop: i64,
    pub with_scores: bool,
}
impl ParseCommand for ZRevRange {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 || args.len() > 4 {
            return Err(SpinelDBError::WrongArgumentCount("ZREVRANGE".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let (start, stop, with_scores) = parse_range_args(args)?;
        Ok(ZRevRange {
            key,
            start,
            stop,
            with_scores,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ZRevRange {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let resp = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                RespValue::Array(vec![])
            } else if let DataValue::SortedSet(zset) = &entry.data {
                let range = zset.get_rev_range(self.start, self.stop);
                format_zrange_response(range, self.with_scores)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            RespValue::Array(vec![])
        };
        Ok((resp, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for ZRevRange {
    fn name(&self) -> &'static str {
        "zrevrange"
    }
    fn arity(&self) -> i64 {
        -4
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
        let mut args = vec![
            self.key.clone(),
            self.start.to_string().into(),
            self.stop.to_string().into(),
        ];
        if self.with_scores {
            args.push("WITHSCORES".into());
        }
        args
    }
}
