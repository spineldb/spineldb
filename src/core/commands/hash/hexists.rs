// src/core/commands/hash/hexists.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct HExists {
    pub key: Bytes,
    pub field: Bytes,
}
impl ParseCommand for HExists {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "HEXISTS")?;
        Ok(HExists {
            key: extract_bytes(&args[0])?,
            field: extract_bytes(&args[1])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for HExists {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let resp = if let Some(entry) = shard_cache_guard.get(&self.key) {
            if entry.is_expired() {
                RespValue::Integer(0)
            } else if let DataValue::Hash(hash) = &entry.data {
                RespValue::Integer(hash.contains_key(&self.field) as i64)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            RespValue::Integer(0)
        };
        Ok((resp, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for HExists {
    fn name(&self) -> &'static str {
        "hexists"
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
        vec![self.key.clone(), self.field.clone()]
    }
}
