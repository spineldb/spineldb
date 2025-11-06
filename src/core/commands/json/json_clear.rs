// src/core/commands/json/json_clear.rs

use super::helpers;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct JsonClear {
    pub key: Bytes,
    pub path: String,
}

impl ParseCommand for JsonClear {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "JSON.CLEAR")?;
        Ok(JsonClear {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonClear {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;

        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &mut entry.data {
            let mut cleared_count = 0;
            let old_size = helpers::estimate_json_memory(root);

            let clear_op = |target: &mut Value| {
                match target {
                    Value::Object(map) if !map.is_empty() => {
                        map.clear();
                        cleared_count += 1;
                    }
                    Value::Array(arr) if !arr.is_empty() => {
                        arr.clear();
                        cleared_count += 1;
                    }
                    Value::String(_) => {
                        *target = Value::String(String::new());
                        cleared_count += 1;
                    }
                    Value::Number(_) => {
                        *target = Value::Number(0.into());
                        cleared_count += 1;
                    }
                    _ => {}
                }
                Ok(Value::Null)
            };

            let find_result = helpers::find_and_modify(root, &path, clear_op, false);

            match find_result {
                Ok(_) => {
                    if cleared_count > 0 {
                        let new_size = helpers::estimate_json_memory(root);
                        let mem_diff = new_size as isize - old_size as isize;

                        entry.version = entry.version.wrapping_add(1);
                        entry.size = new_size;
                        shard.update_memory(mem_diff);

                        Ok((
                            RespValue::Integer(cleared_count),
                            WriteOutcome::Write { keys_modified: 1 },
                        ))
                    } else {
                        Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
                    }
                }
                Err(SpinelDBError::InvalidState(msg)) if msg == "path does not exist" => {
                    Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
                }
                Err(e) => Err(e),
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonClear {
    fn name(&self) -> &'static str {
        "json.clear"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![self.key.clone(), self.path.clone().into()]
    }
}
