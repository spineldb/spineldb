// src/core/commands/zset/zrangebylex.rs

use super::helpers::parse_lex_boundary;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::db::zset::LexBoundary;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZRangeByLex {
    pub key: Bytes,
    pub min: LexBoundary,
    pub max: LexBoundary,
    pub limit: Option<(usize, usize)>,
}

impl ParseCommand for ZRangeByLex {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("ZRANGEBYLEX".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let min = parse_lex_boundary(&extract_string(&args[1])?)?;
        let max = parse_lex_boundary(&extract_string(&args[2])?)?;
        let mut limit = None;
        let mut i = 3;
        if i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            if option == "limit" {
                i += 1;
                if i + 1 >= args.len() {
                    return Err(SpinelDBError::SyntaxError);
                }
                let offset = extract_string(&args[i])?
                    .parse()
                    .map_err(|_| SpinelDBError::NotAnInteger)?;
                i += 1;
                let count = extract_string(&args[i])?
                    .parse()
                    .map_err(|_| SpinelDBError::NotAnInteger)?;
                limit = Some((offset, count));
            } else {
                return Err(SpinelDBError::SyntaxError);
            }
        }
        Ok(ZRangeByLex {
            key,
            min,
            max,
            limit,
        })
    }
}

#[async_trait]
impl ExecutableCommand for ZRangeByLex {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite));
        }

        let DataValue::SortedSet(zset) = &entry.data else {
            return Err(SpinelDBError::WrongType);
        };

        if !zset.scores_are_all_equal() {
            return Err(SpinelDBError::WrongType);
        }

        let mut range_members: Vec<_> = zset
            .get_range_by_lex(&self.min, &self.max)
            .into_iter()
            .map(|e| RespValue::BulkString(e.member))
            .collect();

        if let Some((offset, count)) = self.limit {
            if count > 0 && offset < range_members.len() {
                range_members = range_members.into_iter().skip(offset).take(count).collect();
            } else {
                range_members.clear();
            }
        }

        Ok((RespValue::Array(range_members), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for ZRangeByLex {
    fn name(&self) -> &'static str {
        "zrangebylex"
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
        if let Some((offset, count)) = self.limit {
            args.extend_from_slice(&[
                "LIMIT".into(),
                offset.to_string().into(),
                count.to_string().into(),
            ]);
        }
        args
    }
}
