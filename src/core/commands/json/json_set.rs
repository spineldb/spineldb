// src/core/commands/json/json_set.rs

use super::helpers;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum SetCondition {
    #[default]
    None,
    IfExists,    // XX
    IfNotExists, // NX
}

#[derive(Debug, Clone, Default)]
pub struct JsonSet {
    pub key: Bytes,
    pub path: String,
    pub value_json_str: Bytes,
    pub condition: SetCondition,
}

impl ParseCommand for JsonSet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 || args.len() > 4 {
            return Err(SpinelDBError::WrongArgumentCount("JSON.SET".to_string()));
        }

        let mut cmd = JsonSet {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
            value_json_str: extract_bytes(&args[2])?,
            ..Default::default()
        };

        if let Some(condition_arg) = args.get(3) {
            let condition_str = extract_string(condition_arg)?.to_ascii_uppercase();
            match condition_str.as_str() {
                "NX" => cmd.condition = SetCondition::IfNotExists,
                "XX" => cmd.condition = SetCondition::IfExists,
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for JsonSet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let new_value: serde_json::Value = serde_json::from_slice(&self.value_json_str)
            .map_err(|_| SpinelDBError::InvalidState("Invalid JSON format".to_string()))?;

        let path = helpers::parse_path(&self.path)?;

        let (shard, guard) = ctx.get_single_shard_context_mut()?;

        // For XX, the key must exist.
        if self.condition == SetCondition::IfExists
            && path.is_empty()
            && guard.peek(&self.key).is_none()
        {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        // For NX on a root path, the key must NOT exist.
        if self.condition == SetCondition::IfNotExists
            && path.is_empty()
            && guard.peek(&self.key).is_some()
        {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        let entry = guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::Json(serde_json::Value::Null))
        });

        if let DataValue::Json(root) = &mut entry.data {
            // For non-root paths, check path existence inside the JSON document
            if !path.is_empty() {
                let path_exists = helpers::find_value_by_segments(root, &path).is_some();
                match self.condition {
                    SetCondition::IfExists if !path_exists => {
                        // For XX, if the path does not exist, return null.
                        return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
                    }
                    SetCondition::IfNotExists if path_exists => {
                        // For NX, if the path already exists, return null.
                        return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
                    }
                    _ => {} // For None or conditions that passed, proceed.
                }
            }

            let old_size = helpers::estimate_json_memory(root);

            let set_op = |target: &mut serde_json::Value| {
                *target = new_value.clone();
                Ok(serde_json::Value::Null) // Return value is not used for SET
            };

            if path.is_empty() {
                *root = new_value;
            } else {
                // `create_if_not_exist` should be true for NX and None conditions.
                let create_if_not_exist = self.condition != SetCondition::IfExists;
                helpers::find_and_modify(root, &path, set_op, create_if_not_exist)?;
            }

            let new_size = helpers::estimate_json_memory(root);
            let mem_diff = new_size as isize - old_size as isize;

            entry.version = entry.version.wrapping_add(1);
            entry.size = new_size;
            shard.update_memory(mem_diff);

            Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonSet {
    fn name(&self) -> &'static str {
        "json.set"
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
            self.value_json_str.clone(),
        ]
    }
}
