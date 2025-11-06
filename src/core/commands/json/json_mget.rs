// src/core/commands/json/json_mget.rs

//! Implements the `JSON.MGET` command for retrieving a path from multiple JSON documents.

use super::helpers;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct JsonMGet {
    pub keys: Vec<Bytes>,
    pub path: String,
}

impl ParseCommand for JsonMGet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("JSON.MGET".to_string()));
        }
        let path = extract_string(args.last().unwrap())?;
        let keys = args[..args.len() - 1]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        Ok(JsonMGet { keys, path })
    }
}

#[async_trait]
impl ExecutableCommand for JsonMGet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut results = Vec::with_capacity(self.keys.len());
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "MGET requires multi-key lock".into(),
                ));
            }
        };

        for key in &self.keys {
            let shard_index = ctx.db.get_shard_index(key);
            let value = if let Some(guard) = guards.get(&shard_index) {
                if let Some(entry) = guard.peek(key).filter(|e| !e.is_expired()) {
                    if let DataValue::Json(root) = &entry.data {
                        // For MGET, each key should resolve to a single JSON string response
                        let found_values = helpers::find_values_by_jsonpath(root, &self.path)?;
                        let json_array: Vec<&serde_json::Value> =
                            found_values.into_iter().collect();
                        let response_str = serde_json::to_string(&json_array)?;
                        RespValue::BulkString(response_str.into())
                    } else {
                        // Key exists but is not JSON, MGET returns Null for this key
                        RespValue::Null
                    }
                } else {
                    RespValue::Null
                }
            } else {
                RespValue::Null
            };
            results.push(value);
        }

        Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for JsonMGet {
    fn name(&self) -> &'static str {
        "json.mget"
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
        -2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = self.keys.clone();
        args.push(self.path.clone().into());
        args
    }
}
