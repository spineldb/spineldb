// src/core/commands/json/json_arrindex.rs

use super::helpers;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct JsonArrIndex {
    pub key: Bytes,
    pub path: String,
    pub value_to_find: Value,
    pub start_index: i64,
}

impl ParseCommand for JsonArrIndex {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 || args.len() > 4 {
            return Err(SpinelDBError::WrongArgumentCount(
                "JSON.ARRINDEX".to_string(),
            ));
        }
        let value_to_find: Value = serde_json::from_slice(&extract_bytes(&args[2])?)
            .map_err(|_| SpinelDBError::InvalidState("Invalid JSON value".into()))?;

        let start_index = if args.len() == 4 {
            extract_string(&args[3])?.parse()?
        } else {
            0
        };

        Ok(JsonArrIndex {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
            value_to_find,
            start_index,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonArrIndex {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &entry.data {
            let target_value = helpers::find_value_by_segments(root, &path);

            match target_value {
                Some(Value::Array(arr)) => {
                    let len = arr.len() as i64;
                    let start = if self.start_index >= 0 {
                        self.start_index
                    } else {
                        len + self.start_index
                    }
                    .max(0) as usize;

                    let position = arr
                        .iter()
                        .skip(start)
                        .position(|v| v == &self.value_to_find)
                        .map(|p| (p + start) as i64)
                        .unwrap_or(-1);

                    Ok((RespValue::Integer(position), WriteOutcome::DidNotWrite))
                }
                _ => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonArrIndex {
    fn name(&self) -> &'static str {
        "json.arrindex"
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
            self.path.clone().into(),
            serde_json::to_string(&self.value_to_find)
                .unwrap_or_else(|_| "null".to_string())
                .into(),
        ];
        if self.start_index != 0 {
            args.push(self.start_index.to_string().into());
        }
        args
    }
}
