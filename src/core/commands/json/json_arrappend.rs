// src/core/commands/json/json_arrappend.rs

use super::helpers;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{self, Value};

#[derive(Debug, Clone, Default)]
pub struct JsonArrAppend {
    pub key: Bytes,
    pub path: String,
    pub values: Vec<Bytes>,
}

impl ParseCommand for JsonArrAppend {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount(
                "JSON.ARRAPPEND".to_string(),
            ));
        }
        let key = extract_bytes(&args[0])?;
        let path = extract_string(&args[1])?;
        let values = args[2..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;
        Ok(JsonArrAppend { key, path, values })
    }
}

#[async_trait]
impl ExecutableCommand for JsonArrAppend {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let new_values: Vec<serde_json::Value> = self
            .values
            .iter()
            .map(|v| serde_json::from_slice(v))
            .collect::<Result<_, _>>()
            .map_err(|_| {
                SpinelDBError::InvalidState("Invalid JSON format for value".to_string())
            })?;

        if new_values.is_empty() {
            return Err(SpinelDBError::SyntaxError);
        }

        let path = helpers::parse_path(&self.path)?;

        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        let entry = guard.get_or_insert_with_mut(self.key.clone(), || {
            let mut root = Value::Null;
            if !path.is_empty() {
                let _ = helpers::find_and_modify(
                    &mut root,
                    &path,
                    |v| {
                        *v = Value::Array(vec![]);
                        Ok(Value::Null)
                    },
                    true,
                );
            }
            StoredValue::new(DataValue::Json(root))
        });

        if let DataValue::Json(root) = &mut entry.data {
            let old_size = helpers::estimate_json_memory(root);

            let append_op = |target: &mut Value| {
                if target.is_null() {
                    *target = Value::Array(vec![]);
                }
                if !target.is_array() {
                    return Err(SpinelDBError::InvalidState("Target is not an array".into()));
                }
                let arr = target.as_array_mut().unwrap();
                for val in new_values.iter() {
                    arr.push(val.clone());
                }
                Ok(Value::from(arr.len()))
            };

            let res = helpers::find_and_modify(root, &path, append_op, true)?;

            let final_len = res.as_u64().unwrap_or(0) as i64;

            let new_size = helpers::estimate_json_memory(root);
            let mem_diff = new_size as isize - old_size as isize;

            entry.version = entry.version.wrapping_add(1);
            entry.size = new_size;
            shard.update_memory(mem_diff);

            Ok((
                RespValue::Integer(final_len),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonArrAppend {
    fn name(&self) -> &'static str {
        "json.arrappend"
    }
    fn arity(&self) -> i64 {
        -4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
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
        let mut args = vec![self.key.clone(), self.path.clone().into()];
        args.extend(self.values.clone());
        args
    }
}
