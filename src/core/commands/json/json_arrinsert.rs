// src/core/commands/json/json_arrinsert.rs

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
use serde_json::{self, Value};

#[derive(Debug, Clone, Default)]
pub struct JsonArrInsert {
    pub key: Bytes,
    pub path: String,
    pub index: i64,
    pub values: Vec<Bytes>,
}

impl ParseCommand for JsonArrInsert {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 4 {
            return Err(SpinelDBError::WrongArgumentCount(
                "JSON.ARRINSERT".to_string(),
            ));
        }
        let key = extract_bytes(&args[0])?;
        let path = extract_string(&args[1])?;
        let index = extract_string(&args[2])?.parse()?;
        let values = args[3..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        Ok(JsonArrInsert {
            key,
            path,
            index,
            values,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonArrInsert {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let new_values: Vec<Value> = self
            .values
            .iter()
            .map(|v| serde_json::from_slice(v))
            .collect::<Result<_, _>>()
            .map_err(|_| {
                SpinelDBError::InvalidState("Invalid JSON format for value".to_string())
            })?;

        let path = helpers::parse_path(&self.path)?;

        let (_shard, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get_mut(&self.key) else {
            return Err(SpinelDBError::InvalidState(
                "key or path does not exist".into(),
            ));
        };

        if entry.is_expired() {
            return Err(SpinelDBError::InvalidState(
                "key or path does not exist".into(),
            ));
        }

        if let DataValue::Json(root) = &mut entry.data {
            let mut final_len: i64 = -1;
            let insert_op = |target: &mut Value| {
                if !target.is_array() {
                    return Err(SpinelDBError::InvalidState("Target is not an array".into()));
                }
                let arr = target.as_array_mut().unwrap();
                let len = arr.len();

                let mut insert_pos = if self.index >= 0 {
                    self.index as usize
                } else {
                    (len as i64 + self.index) as usize
                };

                insert_pos = insert_pos.min(len);

                for (i, val) in new_values.iter().enumerate() {
                    arr.insert(insert_pos + i, val.clone());
                }

                final_len = arr.len() as i64;
                Ok(Value::Null)
            };

            helpers::find_and_modify(root, &path, insert_op, false)?;

            if final_len == -1 {
                return Err(SpinelDBError::InvalidState(
                    "key or path does not exist".into(),
                ));
            }

            entry.version = entry.version.wrapping_add(1);
            entry.size = root.to_string().len();

            Ok((
                RespValue::Integer(final_len),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonArrInsert {
    fn name(&self) -> &'static str {
        "json.arrinsert"
    }
    fn arity(&self) -> i64 {
        -5
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
        let mut args = vec![
            self.key.clone(),
            self.path.clone().into(),
            self.index.to_string().into(),
        ];
        args.extend(self.values.clone());
        args
    }
}
