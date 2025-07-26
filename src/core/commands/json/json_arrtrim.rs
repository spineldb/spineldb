// src/core/commands/json/json_arrtrim.rs

use super::helpers;
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
use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct JsonArrTrim {
    pub key: Bytes,
    pub path: String,
    pub start: i64,
    pub stop: i64,
}

impl ParseCommand for JsonArrTrim {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 4, "JSON.ARRTRIM")?;
        Ok(JsonArrTrim {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
            start: extract_string(&args[2])?.parse()?,
            stop: extract_string(&args[3])?.parse()?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonArrTrim {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get_mut(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &mut entry.data {
            let mut final_len: Option<i64> = None;
            let trim_op = |target: &mut Value| {
                if !target.is_array() {
                    return Err(SpinelDBError::InvalidState("Target is not an array".into()));
                }
                let arr = target.as_array_mut().unwrap();
                let len = arr.len() as i64;

                let start = if self.start >= 0 {
                    self.start
                } else {
                    len + self.start
                }
                .max(0) as usize;
                let stop = if self.stop >= 0 {
                    self.stop
                } else {
                    len + self.stop
                }
                .min(len - 1);

                if start as i64 > stop || start >= arr.len() {
                    arr.clear();
                } else {
                    let end_inclusive = (stop + 1) as usize;
                    if end_inclusive < arr.len() {
                        arr.drain(end_inclusive..);
                    }
                    if start > 0 {
                        arr.drain(0..start);
                    }
                }
                final_len = Some(arr.len() as i64);
                Ok(Value::Null)
            };

            helpers::find_and_modify(root, &path, trim_op, false)?;

            if let Some(len) = final_len {
                entry.version = entry.version.wrapping_add(1);
                entry.size = root.to_string().len();
                Ok((
                    RespValue::Integer(len),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Null, WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonArrTrim {
    fn name(&self) -> &'static str {
        "json.arrtrim"
    }
    fn arity(&self) -> i64 {
        5
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
            self.path.clone().into(),
            self.start.to_string().into(),
            self.stop.to_string().into(),
        ]
    }
}
