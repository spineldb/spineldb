// src/core/commands/zset/zlexcount.rs

use super::helpers::parse_lex_boundary;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::database::zset::LexBoundary;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZLexCount {
    pub key: Bytes,
    pub min: LexBoundary,
    pub max: LexBoundary,
}

impl ParseCommand for ZLexCount {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "ZLEXCOUNT")?;
        Ok(ZLexCount {
            key: extract_bytes(&args[0])?,
            min: parse_lex_boundary(&extract_string(&args[1])?)?,
            max: parse_lex_boundary(&extract_string(&args[2])?)?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for ZLexCount {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let DataValue::SortedSet(zset) = &entry.data else {
            return Err(SpinelDBError::WrongType);
        };

        if !zset.scores_are_all_equal() {
            return Err(SpinelDBError::WrongType);
        }

        let count = zset.get_range_by_lex(&self.min, &self.max).len();

        Ok((RespValue::Integer(count as i64), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for ZLexCount {
    fn name(&self) -> &'static str {
        "zlexcount"
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
