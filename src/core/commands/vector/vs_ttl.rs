// src/core/commands/vector/vs_ttl.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.TTL` command to get remaining TTL of a vector index.
///
/// VS.TTL key
/// Returns: -1 if no TTL, -2 if expired/not found, otherwise remaining seconds
#[derive(Debug, Clone, Default)]
pub struct VsTtl {
    pub key: Bytes,
}

impl ParseCommand for VsTtl {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("VS.TTL".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        Ok(VsTtl { key })
    }
}

#[async_trait]
impl ExecutableCommand for VsTtl {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref sv) = entry.data {
            if sv.is_expired() {
                return Ok((RespValue::Integer(-2), WriteOutcome::DidNotWrite));
            }

            match sv.get_remaining_ttl() {
                Some(ttl) => Ok((RespValue::Integer(ttl as i64), WriteOutcome::DidNotWrite)),
                None => Ok((RespValue::Integer(-1), WriteOutcome::DidNotWrite)),
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsTtl {
    fn name(&self) -> &'static str {
        "vs.ttl"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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
    fn test_vsttl_parse_valid() {
        let c = VsTtl::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
    }

    #[test]
    fn test_vsttl_too_few_args() {
        let r = VsTtl::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsttl_too_many_args() {
        let r = VsTtl::parse(&[bs("idx"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsttl_to_resp_args() {
        let c = VsTtl::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"idx")]);
    }
}
