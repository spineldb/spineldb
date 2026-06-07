// src/core/commands/hash/hkeys.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct HKeys {
    pub key: Bytes,
}
impl ParseCommand for HKeys {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "HKEYS")?;
        Ok(HKeys {
            key: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for HKeys {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let resp = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                RespValue::Array(vec![])
            } else if let DataValue::Hash(hash) = &entry.data {
                let keys = hash.keys().cloned().map(RespValue::BulkString).collect();
                RespValue::Array(keys)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            RespValue::Array(vec![])
        };
        Ok((resp, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for HKeys {
    fn name(&self) -> &'static str {
        "hkeys"
    }
    fn arity(&self) -> i64 {
        2
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
        vec![self.key.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_hkeys_parses_key() {
        let c = HKeys::parse(&[bs("h")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"h"));
    }

    #[test]
    fn test_hkeys_with_no_args_is_error() {
        let r = HKeys::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_hkeys_with_too_many_args_is_error() {
        let r = HKeys::parse(&[bs("h"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_hkeys_with_non_bulk_is_wrong_type() {
        let r = HKeys::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_hkeys_to_resp_args_round_trips() {
        let c = HKeys::parse(&[bs("h")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"h")]);
    }
}
