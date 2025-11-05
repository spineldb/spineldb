// src/core/commands/json/json_toggle.rs

//! Implements the `JSON.TOGGLE` command for flipping a boolean value.

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
pub struct JsonToggle {
    pub key: Bytes,
    pub path: String,
}

impl ParseCommand for JsonToggle {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 2 {
            return Err(SpinelDBError::WrongArgumentCount("JSON.TOGGLE".to_string()));
        }
        Ok(JsonToggle {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonToggle {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;
        let (_, guard) = ctx.get_single_shard_context_mut()?;
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
            let mut final_value: Option<bool> = None;
            let toggle_op = |target: &mut Value| {
                let b = target.as_bool().ok_or_else(|| {
                    SpinelDBError::InvalidState("value at path is not a boolean".to_string())
                })?;

                let new_bool = !b;
                *target = Value::Bool(new_bool);
                final_value = Some(new_bool);

                Ok(Value::Null)
            };

            helpers::find_and_modify(root, &path, toggle_op, false)?;

            if let Some(val) = final_value {
                // The memory size of a boolean does not change, so no need to update memory counters.
                entry.version = entry.version.wrapping_add(1);
                Ok((
                    RespValue::Integer(val as i64),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Err(SpinelDBError::InvalidState(
                    "path does not exist".to_string(),
                ))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonToggle {
    fn name(&self) -> &'static str {
        "json.toggle"
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
