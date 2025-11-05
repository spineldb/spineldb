// src/core/commands/json/json_del.rs

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
pub struct JsonDel {
    pub key: Bytes,
    pub paths: Vec<String>,
}

impl ParseCommand for JsonDel {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("JSON.DEL".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let paths = if args.len() > 1 {
            args[1..]
                .iter()
                .map(extract_string)
                .collect::<Result<_, _>>()?
        } else {
            vec!["$".to_string()]
        };
        Ok(JsonDel { key, paths })
    }
}

#[async_trait]
impl ExecutableCommand for JsonDel {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &mut entry.data {
            let mut total_deleted = 0;
            let old_size = helpers::estimate_json_memory(root);

            for path_str in &self.paths {
                if path_str == "$" || path_str == "." {
                    *root = Value::Null;
                    total_deleted = 1;
                    break;
                }

                let path = helpers::parse_path(path_str)?;
                let removed_value = helpers::find_and_remove(root, &path)?;
                if !removed_value.is_null() {
                    total_deleted += 1;
                }
            }

            if total_deleted > 0 {
                let new_size = helpers::estimate_json_memory(root);
                let mem_diff = new_size as isize - old_size as isize;

                entry.version = entry.version.wrapping_add(1);
                entry.size = new_size;
                shard.update_memory(mem_diff);

                Ok((
                    RespValue::Integer(total_deleted),
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

impl CommandSpec for JsonDel {
    fn name(&self) -> &'static str {
        "json.del"
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
        args.extend(self.paths.iter().map(|p| p.clone().into()));
        args
    }
}
