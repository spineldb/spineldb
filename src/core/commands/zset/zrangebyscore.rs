// src/core/commands/zset/zrangebyscore.rs

use super::helpers::{format_zrange_response, parse_score_boundary};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::database::zset::ScoreBoundary;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Limit {
    pub offset: usize,
    pub count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ZRangeByScore {
    pub key: Bytes,
    pub min: ScoreBoundary,
    pub max: ScoreBoundary,
    pub with_scores: bool,
    pub limit: Option<Limit>,
}
impl ParseCommand for ZRangeByScore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount(
                "ZRANGEBYSCORE".to_string(),
            ));
        }
        let key = extract_bytes(&args[0])?;
        let min = parse_score_boundary(&extract_string(&args[1])?)?;
        let max = parse_score_boundary(&extract_string(&args[2])?)?;
        let mut with_scores = false;
        let mut limit = None;
        let mut i = 3;
        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "withscores" => {
                    with_scores = true;
                    i += 1;
                }
                "limit" => {
                    if i + 2 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let offset = extract_string(&args[i + 1])?
                        .parse()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    let count = extract_string(&args[i + 2])?
                        .parse()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    limit = Some(Limit { offset, count });
                    i += 3;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }
        Ok(ZRangeByScore {
            key,
            min,
            max,
            with_scores,
            limit,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ZRangeByScore {
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
                let mut range = zset.get_range_by_score(self.min.clone(), self.max.clone());
                if let Some(limit) = &self.limit {
                    range = range
                        .into_iter()
                        .skip(limit.offset)
                        .take(limit.count)
                        .collect();
                }
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
impl CommandSpec for ZRangeByScore {
    fn name(&self) -> &'static str {
        "zrangebyscore"
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
            self.min.to_string().into(),
            self.max.to_string().into(),
        ];
        if self.with_scores {
            args.push("WITHSCORES".into());
        }
        if let Some(limit) = &self.limit {
            args.push("LIMIT".into());
            args.push(limit.offset.to_string().into());
            args.push(limit.count.to_string().into());
        }
        args
    }
}
