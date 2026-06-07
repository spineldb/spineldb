// src/core/commands/hash/hmget.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct HmGet {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}
impl ParseCommand for HmGet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, fields) = parse_key_and_values(args, 2, "HMGET")?;
        Ok(HmGet { key, fields })
    }
}

#[async_trait]
impl ExecutableCommand for HmGet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let mut responses = Vec::with_capacity(self.fields.len());
        if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if !entry.is_expired() {
                if let DataValue::Hash(hash) = &entry.data {
                    for field in &self.fields {
                        let value = hash
                            .get(field)
                            .cloned()
                            .map(RespValue::BulkString)
                            .unwrap_or(RespValue::Null);
                        responses.push(value);
                    }
                } else {
                    return Err(SpinelDBError::WrongType);
                }
            } else {
                shard_cache_guard.pop(&self.key);
                for _ in &self.fields {
                    responses.push(RespValue::Null);
                }
            }
        } else {
            for _ in &self.fields {
                responses.push(RespValue::Null);
            }
        }
        Ok((RespValue::Array(responses), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for HmGet {
    fn name(&self) -> &'static str {
        "hmget"
    }
    fn arity(&self) -> i64 {
        -3
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
        args.extend(self.fields.clone());
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
    fn test_hmget_parses_key_and_fields() {
        let c = HmGet::parse(&[bs("h"), bs("f1"), bs("f2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"h"));
        assert_eq!(c.fields.len(), 2);
        assert_eq!(c.fields[0], Bytes::from_static(b"f1"));
        assert_eq!(c.fields[1], Bytes::from_static(b"f2"));
    }

    #[test]
    fn test_hmget_with_too_few_args_is_error() {
        let r = HmGet::parse(&[bs("h")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_hmget_with_non_bulk_key_is_wrong_type() {
        let r = HmGet::parse(&[RespFrame::Integer(1), bs("f")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_hmget_with_non_bulk_field_is_wrong_type() {
        let r = HmGet::parse(&[bs("h"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_hmget_to_resp_args_round_trips() {
        let c = HmGet::parse(&[bs("h"), bs("a"), bs("b"), bs("c")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[0], Bytes::from_static(b"h"));
        assert_eq!(args[1], Bytes::from_static(b"a"));
        assert_eq!(args[3], Bytes::from_static(b"c"));
    }
}
