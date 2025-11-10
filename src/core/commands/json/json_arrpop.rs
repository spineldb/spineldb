// src/core/commands/json/json_arrpop.rs

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
pub struct JsonArrPop {
    pub key: Bytes,
    pub path: Option<String>,
    pub index: Option<i64>,
}

impl ParseCommand for JsonArrPop {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 3 {
            return Err(SpinelDBError::WrongArgumentCount("JSON.ARRPOP".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let mut path = None;
        let mut index = None;

        if args.len() > 1 {
            path = Some(extract_string(&args[1])?);
        }
        if args.len() > 2 {
            index = Some(extract_string(&args[2])?.parse()?);
        }

        Ok(JsonArrPop { key, path, index })
    }
}

#[async_trait]
impl ExecutableCommand for JsonArrPop {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path_str = self.path.as_deref().unwrap_or(".");
        let path = helpers::parse_path(path_str)?;

        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get_mut(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            guard.pop(&self.key); // Passive deletion
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &mut entry.data {
            let mut popped_value = Value::Null;
            let old_size = helpers::estimate_json_memory(root);

            let pop_op = |target: &mut Value| {
                match target.as_array_mut() {
                    Some(arr) => {
                        if arr.is_empty() {
                            return Ok(Value::Null); // Nothing to pop
                        }

                        let len = arr.len();
                        let index_to_pop = self.index.unwrap_or(-1); // Default is to pop from the end

                        let final_index = if index_to_pop >= 0 {
                            index_to_pop as usize
                        } else {
                            (len as i64 + index_to_pop) as usize
                        };

                        // Explicitly check for out-of-bounds access.
                        if final_index >= len {
                            return Ok(Value::Null); // Index out of bounds, nothing to pop.
                        }

                        popped_value = arr.remove(final_index);
                        Ok(popped_value.clone())
                    }
                    None => Err(SpinelDBError::WrongType),
                }
            };

            // find_and_modify returns an error if the path does not exist.
            if helpers::find_and_modify(root, &path, pop_op, false).is_err() {
                return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
            }

            if popped_value.is_null() {
                return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
            }

            let new_size = helpers::estimate_json_memory(root);
            let mem_diff = new_size as isize - old_size as isize;

            // Update metadata as the value has changed.
            entry.version = entry.version.wrapping_add(1);
            entry.size = new_size;
            shard.update_memory(mem_diff);

            // Always serialize the popped value to a JSON string, as per RedisJSON behavior.
            let response_str = serde_json::to_string(&popped_value)?;
            let response_value = RespValue::BulkString(response_str.into());

            Ok((response_value, WriteOutcome::Write { keys_modified: 1 }))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonArrPop {
    fn name(&self) -> &'static str {
        "json.arrpop"
    }
    fn arity(&self) -> i64 {
        -2
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
        let mut args = vec![self.key.clone()];
        if let Some(p) = &self.path {
            args.push(p.clone().into());
        }
        if let Some(i) = self.index {
            args.push(i.to_string().into());
        }
        args
    }
}
