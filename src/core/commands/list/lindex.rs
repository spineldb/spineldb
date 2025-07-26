// src/core/commands/list/lindex.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct LIndex {
    pub key: Bytes,
    pub index: i64,
}
impl ParseCommand for LIndex {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "LINDEX")?;
        Ok(LIndex {
            key: extract_bytes(&args[0])?,
            index: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for LIndex {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Use consistent helper and handle passive expiration.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                // Passively delete expired key.
                shard_cache_guard.pop(&self.key);
                return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
            }
            if let DataValue::List(list) = &entry.data {
                let len = list.len() as i64;
                let index = if self.index >= 0 {
                    self.index
                } else {
                    len + self.index
                };
                if index < 0 || index >= len {
                    return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
                }
                let value = list
                    .get(index as usize)
                    .cloned()
                    .map(RespValue::BulkString)
                    .unwrap_or(RespValue::Null);
                return Ok((value, WriteOutcome::DidNotWrite));
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }
        Ok((RespValue::Null, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for LIndex {
    fn name(&self) -> &'static str {
        "lindex"
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
        vec![self.key.clone(), self.index.to_string().into()]
    }
}
