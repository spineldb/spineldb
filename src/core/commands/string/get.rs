// src/core/commands/string/get.rs

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
pub struct Get {
    pub key: Bytes,
}

impl ParseCommand for Get {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "GET")?;
        Ok(Get {
            key: extract_bytes(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Get {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // `get_mut` from the guard will automatically update LFU/LRU info.
        if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                // To avoid borrowing issues, we get the key again to pop it.
                let key_clone = self.key.clone();
                shard_cache_guard.pop(&key_clone);
                Ok((RespValue::Null, WriteOutcome::DidNotWrite))
            } else {
                match &entry.data {
                    DataValue::String(s) => {
                        Ok((RespValue::BulkString(s.clone()), WriteOutcome::DidNotWrite))
                    }
                    _ => Err(SpinelDBError::WrongType),
                }
            }
        } else {
            Ok((RespValue::Null, WriteOutcome::DidNotWrite))
        }
    }
}

impl CommandSpec for Get {
    fn name(&self) -> &'static str {
        "get"
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
    fn test_get_parse_basic() {
        let c = Get::parse(&[bs("mykey")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"mykey"));
    }

    #[test]
    fn test_get_parse_empty_args_is_error() {
        let r = Get::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_get_parse_non_bulk_string_is_error() {
        let r = Get::parse(&[RespFrame::Integer(123)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_get_to_resp_args() {
        let c = Get {
            key: Bytes::from_static(b"counter"),
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], Bytes::from_static(b"counter"));
    }

    #[test]
    fn test_get_spec() {
        let c = Get {
            key: Bytes::from_static(b"k"),
        };
        assert_eq!(c.name(), "get");
        assert_eq!(c.arity(), 2);
        assert!(c.flags().contains(CommandFlags::READONLY));
        assert_eq!(c.first_key(), 1);
        assert_eq!(c.step(), 1);
    }
}
