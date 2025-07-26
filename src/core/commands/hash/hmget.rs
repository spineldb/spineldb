// src/core/commands/hash/hmget.rs

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

#[derive(Debug, Clone, Default)]
pub struct HmGet {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}
impl ParseCommand for HmGet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, fields) = parse_key_and_values(args, 2, "HMGET")?;
        Ok(HmGet { key, fields })
    }
}

#[async_trait]
impl ExecutableCommand for HmGet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let mut responses = Vec::with_capacity(self.fields.len());
        if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if !entry.is_expired() {
                if let DataValue::Hash(hash) = &entry.data {
                    for field in &self.fields {
                        let value = hash
                            .get(field)
                            .cloned()
                            .map(RespValue::BulkString)
                            .unwrap_or(RespValue::Null);
                        responses.push(value);
                    }
                } else {
                    return Err(SpinelDBError::WrongType);
                }
            } else {
                shard_cache_guard.pop(&self.key);
                for _ in &self.fields {
                    responses.push(RespValue::Null);
                }
            }
        } else {
            for _ in &self.fields {
                responses.push(RespValue::Null);
            }
        }
        Ok((RespValue::Array(responses), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for HmGet {
    fn name(&self) -> &'static str {
        "hmget"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone()];
        args.extend(self.fields.clone());
        args
    }
}
