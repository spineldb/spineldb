// src/core/commands/list/lindex.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct LIndex {
    pub key: Bytes,
    pub index: i64,
}
impl ParseCommand for LIndex {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "LINDEX")?;
        Ok(LIndex {
            key: extract_bytes(&args[0])?,
            index: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for LIndex {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Use consistent helper and handle passive expiration.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                // Passively delete expired key.
                shard_cache_guard.pop(&self.key);
                return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
            }
            if let DataValue::List(list) = &entry.data {
                let len = list.len() as i64;
                let index = if self.index >= 0 {
                    self.index
                } else {
                    len + self.index
                };
                if index < 0 || index >= len {
                    return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
                }
                let value = list
                    .get(index as usize)
                    .cloned()
                    .map(RespValue::BulkString)
                    .unwrap_or(RespValue::Null);
                return Ok((value, WriteOutcome::DidNotWrite));
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }
        Ok((RespValue::Null, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for LIndex {
    fn name(&self) -> &'static str {
        "lindex"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![self.key.clone(), self.index.to_string().into()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_lindex_parses_key_and_index() {
        let c = LIndex::parse(&[bs("k"), bs("0")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.index, 0);
    }

    #[test]
    fn test_lindex_parses_negative_index() {
        let c = LIndex::parse(&[bs("k"), bs("-1")]).unwrap();
        assert_eq!(c.index, -1);
    }

    #[test]
    fn test_lindex_with_too_few_args_is_error() {
        let r = LIndex::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lindex_with_too_many_args_is_error() {
        let r = LIndex::parse(&[bs("k"), bs("0"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lindex_with_non_integer_index_is_error() {
        let r = LIndex::parse(&[bs("k"), bs("first")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_lindex_to_resp_args_round_trips() {
        let c = LIndex::parse(&[bs("k"), bs("5")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(
            args,
            vec![Bytes::from_static(b"k"), Bytes::from_static(b"5")]
        );
    }
}
