// src/core/commands/generic/type_cmd.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// A command for getting the data type of a key.
/// Corresponds to the `TYPE` Redis command.
#[derive(Debug, Clone, Default)]
pub struct TypeInfo {
    pub key: Bytes,
}

impl ParseCommand for TypeInfo {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "TYPE")?;
        Ok(TypeInfo {
            key: extract_bytes(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for TypeInfo {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let shard_cache_guard = match &mut ctx.locks {
            ExecutionLocks::Single { guard, .. } => guard,
            ExecutionLocks::Multi { guards } => {
                let shard_index = ctx.db.get_shard_index(&self.key);
                guards.get_mut(&shard_index).ok_or_else(|| {
                    SpinelDBError::Internal("Mismatched lock in multi-key command for TYPE".into())
                })?
            }
            _ => return Err(SpinelDBError::Internal("TYPE requires a shard lock".into())),
        };

        if let Some(entry) = shard_cache_guard.peek(&self.key) {
            if entry.is_expired() {
                // An expired key is treated as non-existent.
                return Ok((
                    RespValue::SimpleString("none".into()),
                    WriteOutcome::DidNotWrite,
                ));
            }
            // Determine the type name based on the DataValue variant.
            let type_name = match &entry.data {
                DataValue::String(_) => "string",
                DataValue::List(_) => "list",
                DataValue::Set(_) => "set",
                DataValue::SortedSet(_) => "zset",
                DataValue::Hash(_) => "hash",
                DataValue::Stream(_) => "stream",
                DataValue::Json(_) => "json",
                DataValue::HyperLogLog(_) => "hyperloglog",
                DataValue::BloomFilter(_) => "bloomfilter",
                // For compatibility, an HttpCache is exposed as a "string" type
                // to clients, as they primarily interact with its body.
                DataValue::HttpCache { .. } => "string",
            };
            Ok((
                RespValue::SimpleString(type_name.to_string()),
                WriteOutcome::DidNotWrite,
            ))
        } else {
            // If the key does not exist, return "none".
            Ok((
                RespValue::SimpleString("none".into()),
                WriteOutcome::DidNotWrite,
            ))
        }
    }
}

impl CommandSpec for TypeInfo {
    fn name(&self) -> &'static str {
        "type"
    }
    fn arity(&self) -> i64 {
        2
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
        vec![self.key.clone()]
    }
}
