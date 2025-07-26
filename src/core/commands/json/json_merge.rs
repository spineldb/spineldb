// src/core/commands/json/json_merge.rs
//! Implements the `JSON.MERGE` command for merging a JSON object or array.

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
pub struct JsonMerge {
    pub key: Bytes,
    pub path: String,
    pub value: Bytes,
}

impl ParseCommand for JsonMerge {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "JSON.MERGE")?;
        Ok(JsonMerge {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
            value: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonMerge {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;
        let merge_value: Value = serde_json::from_slice(&self.value).map_err(|_| {
            SpinelDBError::InvalidState("Invalid JSON format for value".to_string())
        })?;

        // MERGE only works on container types (object, array).
        if !merge_value.is_object() && !merge_value.is_array() {
            return Err(SpinelDBError::InvalidState(
                "Merge value must be a JSON object or array".to_string(),
            ));
        }

        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &mut entry.data {
            let mut merged = false;

            let merge_op = |target: &mut Value| {
                match (target, &merge_value) {
                    // Merge an object into another object.
                    (Value::Object(target_obj), Value::Object(merge_obj)) => {
                        for (k, v) in merge_obj {
                            target_obj.insert(k.clone(), v.clone());
                        }
                        merged = true;
                    }
                    // Append elements of an array to another array.
                    (Value::Array(target_arr), Value::Array(merge_arr)) => {
                        target_arr.extend_from_slice(merge_arr);
                        merged = true;
                    }
                    // All other type combinations are invalid for MERGE.
                    _ => {
                        return Err(SpinelDBError::InvalidState(
                            "Cannot merge values of different types".to_string(),
                        ));
                    }
                }
                Ok(Value::Null)
            };

            helpers::find_and_modify(root, &path, merge_op, false)?;

            if merged {
                entry.version = entry.version.wrapping_add(1);
                entry.size = root.to_string().len();
                Ok((
                    RespValue::Integer(1), // Redis returns 1 on successful merge.
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonMerge {
    fn name(&self) -> &'static str {
        "json.merge"
    }
    fn arity(&self) -> i64 {
        4
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
            self.value.clone(),
        ]
    }
}
