// src/core/commands/json/json_objkeys.rs

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
pub struct JsonObjKeys {
    pub key: Bytes,
    pub path: Option<String>,
}

impl ParseCommand for JsonObjKeys {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 2 {
            return Err(SpinelDBError::WrongArgumentCount(
                "JSON.OBJKEYS".to_string(),
            ));
        }
        let key = extract_bytes(&args[0])?;
        let path = if args.len() == 2 {
            Some(extract_string(&args[1])?)
        } else {
            None
        };
        Ok(JsonObjKeys { key, path })
    }
}

#[async_trait]
impl ExecutableCommand for JsonObjKeys {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let path_str = self.path.as_deref().unwrap_or("$");
        let path = helpers::parse_path(path_str)?;

        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.get(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Json(root) = &entry.data {
            let target_value = helpers::find_value_by_segments(root, &path);
            match target_value {
                Some(Value::Object(map)) => {
                    let keys: Vec<RespValue> = map
                        .keys()
                        .map(|k| RespValue::BulkString(k.clone().into()))
                        .collect();
                    Ok((RespValue::Array(keys), WriteOutcome::DidNotWrite))
                }
                Some(_) => Err(SpinelDBError::InvalidState(
                    "Target is not an object".into(),
                )),
                None => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for JsonObjKeys {
    fn name(&self) -> &'static str {
        "json.objkeys"
    }
    fn arity(&self) -> i64 {
        -2
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
        if let Some(p) = &self.path {
            args.push(p.clone().into());
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_json_objkeys_key_only() {
        let c = JsonObjKeys::parse(&[bs("k")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert!(c.path.is_none());
    }

    #[test]
    fn test_json_objkeys_with_path() {
        let c = JsonObjKeys::parse(&[bs("k"), bs("$.o")]).unwrap();
        assert_eq!(c.path, Some("$.o".to_string()));
    }

    #[test]
    fn test_json_objkeys_no_args_is_error() {
        let r = JsonObjKeys::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_json_objkeys_too_many_args_is_error() {
        let r = JsonObjKeys::parse(&[bs("k"), bs("$.o"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_json_objkeys_with_non_bulk_key_is_wrong_type() {
        let r = JsonObjKeys::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_json_objkeys_to_resp_args_round_trips() {
        let c = JsonObjKeys::parse(&[bs("k"), bs("$.o")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args, vec![Bytes::from_static(b"k"), Bytes::from_static(b"$.o")]);
    }
}
