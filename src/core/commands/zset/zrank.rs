// src/core/commands/zset/zrank.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZRank {
    pub key: Bytes,
    pub member: Bytes,
}
impl ParseCommand for ZRank {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "ZRANK")?;
        Ok(ZRank {
            key: extract_bytes(&args[0])?,
            member: extract_bytes(&args[1])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ZRank {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
            }
            if let DataValue::SortedSet(zset) = &entry.data {
                if let Some(rank) = zset.get_rank(&self.member) {
                    Ok((RespValue::Integer(rank as i64), WriteOutcome::DidNotWrite))
                } else {
                    Ok((RespValue::Null, WriteOutcome::DidNotWrite))
                }
            } else {
                Err(SpinelDBError::WrongType)
            }
        } else {
            Ok((RespValue::Null, WriteOutcome::DidNotWrite))
        }
    }
}

impl CommandSpec for ZRank {
    fn name(&self) -> &'static str {
        "zrank"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![self.key.clone(), self.member.clone()]
    }
}
