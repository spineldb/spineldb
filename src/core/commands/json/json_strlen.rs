// src/core/commands/json/json_strlen.rs

use super::helpers;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct JsonStrLen {
    pub key: Bytes,
    pub path: String,
}

impl ParseCommand for JsonStrLen {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 2 {
            return Err(SpinelDBError::WrongArgumentCount("JSON.STRLEN".to_string()));
        }
        Ok(JsonStrLen {
            key: extract_bytes(&args[0])?,
            path: if args.len() == 2 {
                extract_string(&args[1])?
            } else {
                "$".to_string()
            },
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonStrLen {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &entry.data {
            let found = helpers::find_values_by_jsonpath(root, &self.path)?;
            if found.is_empty() {
                return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
            }
            if found.len() > 1 {
                return Err(SpinelDBError::InvalidState(
                    "path must be a single path to a string".to_string(),
                ));
            }
            match found[0] {
                Value::String(s) => Ok((
                    RespValue::Integer(s.len() as i64),
                    WriteOutcome::DidNotWrite,
                )),
                _ => Err(SpinelDBError::InvalidState(
                    "value at path is not a string".to_string(),
                )),
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonStrLen {
    fn name(&self) -> &'static str {
        "json.strlen"
    }
    fn arity(&self) -> i64 {
        -2
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
        vec![self.key.clone(), self.path.clone().into()]
    }
}
