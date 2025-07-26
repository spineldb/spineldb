// src/core/commands/json/json_arrlen.rs

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
pub struct JsonArrLen {
    pub key: Bytes,
    pub path: Option<String>,
}

impl ParseCommand for JsonArrLen {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 2 {
            return Err(SpinelDBError::WrongArgumentCount("JSON.ARRLEN".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let path = if args.len() == 2 {
            Some(extract_string(&args[1])?)
        } else {
            None
        };
        Ok(JsonArrLen { key, path })
    }
}

#[async_trait]
impl ExecutableCommand for JsonArrLen {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path_str = self.path.as_deref().unwrap_or("$");
        let path = helpers::parse_path(path_str)?;

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
                Some(Value::Array(arr)) => Ok((
                    RespValue::Integer(arr.len() as i64),
                    WriteOutcome::DidNotWrite,
                )),
                Some(_) => Err(SpinelDBError::InvalidState("Target is not an array".into())),
                None => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonArrLen {
    fn name(&self) -> &'static str {
        "json.arrlen"
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
        let mut args = vec![self.key.clone()];
        if let Some(p) = &self.path {
            args.push(p.clone().into());
        }
        args
    }
}
