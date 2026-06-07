// src/core/commands/generic/ttl.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Ttl {
    pub key: Bytes,
}
impl ParseCommand for Ttl {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "TTL")?;
        Ok(Ttl {
            key: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for Ttl {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let result = if let Some(entry) = shard_cache_guard.get(&self.key) {
            if entry.is_expired() {
                -2 // Key exists but is expired.
            } else if let Some(ttl) = entry.remaining_ttl_secs() {
                // Key exists and has a TTL.
                ttl as i64
            } else {
                // Key exists but has no TTL.
                -1
            }
        } else {
            // Key does not exist.
            -2
        };
        Ok((RespValue::Integer(result), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Ttl {
    fn name(&self) -> &'static str {
        "ttl"
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
    fn test_ttl_parses_key() {
        let t = Ttl::parse(&[bs("k")]).unwrap();
        assert_eq!(t.key, Bytes::from_static(b"k"));
    }

    #[test]
    fn test_ttl_with_no_args_is_error() {
        let r = Ttl::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_ttl_with_too_many_args_is_error() {
        let r = Ttl::parse(&[bs("k"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_ttl_with_non_bulk_is_wrong_type() {
        let r = Ttl::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_ttl_to_resp_args_round_trips() {
        let t = Ttl::parse(&[bs("mykey")]).unwrap();
        let args = t.to_resp_args();
        assert_eq!(args, vec![Bytes::from_static(b"mykey")]);
    }
}
