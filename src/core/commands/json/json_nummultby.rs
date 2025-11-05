// src/core/commands/json/json_nummultby.rs

//! Implements the `JSON.NUMMULTBY` command for multiplying a numeric value.

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
use serde_json::{Number, Value};

#[derive(Debug, Clone, Default)]
pub struct JsonNumMultBy {
    pub key: Bytes,
    pub path: String,
    pub value: f64,
}

impl ParseCommand for JsonNumMultBy {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "JSON.NUMMULTBY")?;
        Ok(JsonNumMultBy {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
            value: extract_string(&args[2])?.parse()?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonNumMultBy {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;
        let (shard, guard) = ctx.get_single_shard_context_mut()?;
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
            let mut final_value_str = String::new();
            let old_size = helpers::estimate_json_memory(root);

            let mult_op = |target: &mut Value| {
                let current_val = target.as_f64().ok_or(SpinelDBError::NotAFloat)?;
                let new_val_float = current_val * self.value;
                *target =
                    Value::Number(Number::from_f64(new_val_float).ok_or(SpinelDBError::NotAFloat)?);
                final_value_str = helpers::format_json_number(target.as_number().unwrap());
                Ok(Value::Null)
            };

            helpers::find_and_modify(root, &path, mult_op, false)?;

            let new_size = helpers::estimate_json_memory(root);
            let mem_diff = new_size as isize - old_size as isize;

            entry.version = entry.version.wrapping_add(1);
            entry.size = new_size;
            shard.update_memory(mem_diff);

            Ok((
                RespValue::BulkString(final_value_str.into()),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonNumMultBy {
    fn name(&self) -> &'static str {
        "json.nummultby"
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
            self.value.to_string().into(),
        ]
    }
}
