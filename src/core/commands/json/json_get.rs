// src/core/commands/json/json_get.rs

//! Implements the `JSON.GET` command for retrieving values from a JSON document.

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

/// Represents the `JSON.GET` command.
#[derive(Debug, Clone, Default)]
pub struct JsonGet {
    /// The key of the JSON document.
    pub key: Bytes,
    /// One or more JSONPath expressions to query.
    pub paths: Vec<String>,
}

impl ParseCommand for JsonGet {
    /// Parses the `JSON.GET` command arguments from a slice of `RespFrame`.
    /// Syntax: `JSON.GET <key> [path...]`
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("JSON.GET".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        // If no paths are provided, default to getting the root element ("$").
        let paths = if args.len() > 1 {
            args[1..]
                .iter()
                .map(extract_string)
                .collect::<Result<_, _>>()?
        } else {
            vec!["$".to_string()]
        };
        Ok(JsonGet { key, paths })
    }
}

#[async_trait]
impl ExecutableCommand for JsonGet {
    /// Executes the `JSON.GET` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = guard.get(&self.key) else {
            // If the key doesn't exist, the response is Null.
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &entry.data {
            // --- Single Path Logic ---
            if self.paths.len() == 1 {
                let path_str = &self.paths[0];
                let found_values = helpers::find_values_by_jsonpath(root, path_str)?;

                // According to RedisJSON, if a single path query returns multiple values,
                // they are wrapped in a JSON array string. If it returns one value,
                // that value is returned directly. If none, null is returned.
                // This logic handles all cases cleanly.
                let response = match found_values.len() {
                    0 => RespValue::Null,
                    1 => {
                        // The JSON value is serialized to a string for the client.
                        let json_str = serde_json::to_string(found_values[0])?;
                        RespValue::BulkString(json_str.into())
                    }
                    _ => {
                        let json_array: Vec<&Value> = found_values.into_iter().collect();
                        let response_str = serde_json::to_string(&json_array)?;
                        RespValue::BulkString(response_str.into())
                    }
                };
                return Ok((response, WriteOutcome::DidNotWrite));
            }

            // --- Multiple Path Logic ---
            // The result is a single JSON object string where keys are paths.
            let mut results_map = serde_json::Map::new();
            for path_str in &self.paths {
                let found_values = helpers::find_values_by_jsonpath(root, path_str)?;

                // If a path yields results, wrap them in a JSON array.
                // If not, the value for that path is JSON null.
                let result_value = if !found_values.is_empty() {
                    let json_array: Vec<&Value> = found_values.into_iter().collect();
                    Value::Array(json_array.into_iter().cloned().collect())
                } else {
                    Value::Null
                };
                results_map.insert(path_str.clone(), result_value);
            }

            let response_json = Value::Object(results_map);
            let response_str = serde_json::to_string(&response_json)?;

            Ok((
                RespValue::BulkString(response_str.into()),
                WriteOutcome::DidNotWrite,
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonGet {
    fn name(&self) -> &'static str {
        "json.get"
    }

    fn arity(&self) -> i64 {
        -2 // key [path...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
