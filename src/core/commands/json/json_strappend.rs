// src/core/commands/json/json_strappend.rs

//! Implements the `JSON.STRAPPEND` command for appending a string to a JSON string value.

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
pub struct JsonStrAppend {
    pub key: Bytes,
    pub path: String,
    pub value_to_append: String,
}

impl ParseCommand for JsonStrAppend {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 3 {
            return Err(SpinelDBError::WrongArgumentCount(
                "JSON.STRAPPEND".to_string(),
            ));
        }
        Ok(JsonStrAppend {
            key: extract_bytes(&args[0])?,
            path: extract_string(&args[1])?,
            // The value to append is a JSON string literal, so we parse it
            value_to_append: {
                let json_str_arg = extract_string(&args[2])?;
                let parsed_value: Value = serde_json::from_str(&json_str_arg).map_err(|_| {
                    SpinelDBError::InvalidState(
                        "Value to append must be a valid JSON string".to_string(),
                    )
                })?;
                parsed_value
                    .as_str()
                    .ok_or_else(|| {
                        SpinelDBError::InvalidState(
                            "Value to append must be a JSON string".to_string(),
                        )
                    })?
                    .to_string()
            },
        })
    }
}

#[async_trait]
impl ExecutableCommand for JsonStrAppend {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path = helpers::parse_path(&self.path)?;
        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get_mut(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &mut entry.data {
            let mut final_len: Option<i64> = None;
            let old_size = helpers::estimate_json_memory(root);

            let append_op = |target: &mut Value| {
                let original_str = target.as_str().ok_or_else(|| {
                    SpinelDBError::InvalidState("value at path is not a string".to_string())
                })?;

                let mut new_string = original_str.to_owned();
                new_string.push_str(&self.value_to_append);

                final_len = Some(new_string.len() as i64);
                *target = Value::String(new_string);

                Ok(Value::Null)
            };

            helpers::find_and_modify(root, &path, append_op, false)?;

            if let Some(len) = final_len {
                let new_size = helpers::estimate_json_memory(root);
                let mem_diff = new_size as isize - old_size as isize;

                entry.version = entry.version.wrapping_add(1);
                entry.size = new_size;
                shard.update_memory(mem_diff);

                Ok((
                    RespValue::Integer(len),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Null, WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonStrAppend {
    fn name(&self) -> &'static str {
        "json.strappend"
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
            self.value_to_append.clone().into(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_json_strappend_parses() {
        let c = JsonStrAppend::parse(&[bs("k"), bs("$.a"), bs("\"suffix\"")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.path, "$.a");
        assert_eq!(c.value_to_append, "suffix");
    }

    #[test]
    fn test_json_strappend_with_empty_string() {
        let c = JsonStrAppend::parse(&[bs("k"), bs("$.a"), bs("\"\"")]).unwrap();
        assert_eq!(c.value_to_append, "");
    }

    #[test]
    fn test_json_strappend_too_few_args_is_error() {
        let r = JsonStrAppend::parse(&[bs("k"), bs("$.a")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_json_strappend_too_many_args_is_error() {
        let r = JsonStrAppend::parse(&[bs("k"), bs("$.a"), bs("\"x\""), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_json_strappend_no_args_is_error() {
        let r = JsonStrAppend::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_json_strappend_invalid_json_value_is_error() {
        let r = JsonStrAppend::parse(&[bs("k"), bs("$.a"), bs("not_a_json_string")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidState(_))));
    }

    #[test]
    fn test_json_strappend_non_string_json_value_is_error() {
        let r = JsonStrAppend::parse(&[bs("k"), bs("$.a"), bs("42")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidState(_))));
    }

    #[test]
    fn test_json_strappend_with_non_bulk_key_is_wrong_type() {
        let r = JsonStrAppend::parse(&[RespFrame::Integer(1), bs("$.a"), bs("\"x\"")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_json_strappend_to_resp_args_round_trips() {
        let c = JsonStrAppend::parse(&[bs("k"), bs("$.a"), bs("\"x\"")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(
            args,
            vec![
                Bytes::from_static(b"k"),
                Bytes::from_static(b"$.a"),
                Bytes::from_static(b"x"),
            ]
        );
    }
}
