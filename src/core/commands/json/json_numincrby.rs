// src/core/commands/json/json_numincrby.rs

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
use serde_json::{Number, Value};

/// Represents the `JSON.NUMINCRBY` command, which increments a numeric value
/// within a JSON document.
#[derive(Debug, Clone, Default)]
pub struct JsonNumIncrBy {
    pub key: Bytes,
    pub path: String,
    pub value: f64,
}

impl ParseCommand for JsonNumIncrBy {
    /// Parses the `JSON.NUMINCRBY` command arguments from a slice of `RespFrame`.
    /// `JSON.NUMINCRBY <key> <path> <number>`
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "JSON.NUMINCRBY")?;
        Ok(JsonNumIncrBy {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
            value: extract_string(&args[2])?.parse()?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonNumIncrBy {
    /// Executes the `JSON.NUMINCRBY` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;

        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        // This command requires the key and path to already exist.
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

            // Define the increment operation as a closure.
            let incr_op = |target: &mut Value| {
                let current_val = match target {
                    Value::Number(n) => n.as_f64().ok_or(SpinelDBError::NotAFloat)?,
                    _ => {
                        return Err(SpinelDBError::InvalidState(
                            "value is not a number".to_string(),
                        ));
                    }
                };

                let new_val_float = current_val + self.value;

                // Check if the result is a whole number and within i64 range.
                // If so, store it as an i64 to preserve its integer type.
                if new_val_float.fract() == 0.0
                    && new_val_float <= i64::MAX as f64
                    && new_val_float >= i64::MIN as f64
                {
                    let new_val_int = new_val_float as i64;
                    *target = Value::Number(Number::from(new_val_int));
                    final_value_str = new_val_int.to_string();
                } else {
                    // Otherwise, store it as an f64.
                    *target = Value::Number(
                        Number::from_f64(new_val_float).ok_or(SpinelDBError::NotAFloat)?,
                    );
                    final_value_str = helpers::format_json_number(target.as_number().unwrap());
                }

                Ok(Value::Null) // The return value of the op is not used directly.
            };

            // Traverse the JSON document and apply the operation.
            // `create_if_not_exist` is `false` because the path must exist.
            helpers::find_and_modify(root, &path, incr_op, false)?;

            let new_size = helpers::estimate_json_memory(root);
            let mem_diff = new_size as isize - old_size as isize;

            // Update metadata since the value has changed.
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

impl CommandSpec for JsonNumIncrBy {
    fn name(&self) -> &'static str {
        "json.numincrby"
    }

    fn arity(&self) -> i64 {
        4
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
        vec![
            self.key.clone(),
            self.path.clone().into(),
            self.value.to_string().into(),
        ]
    }
}
