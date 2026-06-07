// src/core/commands/list/llen.rs

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
pub struct LLen {
    pub key: Bytes,
}
impl ParseCommand for LLen {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "LLEN")?;
        Ok(LLen {
            key: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for LLen {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let len = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                0
            } else if let DataValue::List(list) = &entry.data {
                list.len()
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            0
        };
        Ok((RespValue::Integer(len as i64), WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for LLen {
    fn name(&self) -> &'static str {
        "llen"
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
    fn test_llen_parses_key() {
        let c = LLen::parse(&[bs("mylist")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"mylist"));
    }

    #[test]
    fn test_llen_with_no_args_is_error() {
        let r = LLen::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_llen_with_too_many_args_is_error() {
        let r = LLen::parse(&[bs("k"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_llen_with_non_bulk_is_wrong_type() {
        let r = LLen::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_llen_to_resp_args_round_trips() {
        let c = LLen::parse(&[bs("k")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"k")]);
    }
}
