// src/core/commands/string/mget.rs
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct MGet {
    pub keys: Vec<Bytes>,
}
impl ParseCommand for MGet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("MGET".to_string()));
        }
        let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(MGet { keys })
    }
}
#[async_trait]
impl ExecutableCommand for MGet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut responses = Vec::with_capacity(self.keys.len());
        if let ExecutionLocks::Multi { guards } = &ctx.locks {
            for key in &self.keys {
                let shard_index = ctx.db.get_shard_index(key);
                let value = if let Some(guard) = guards.get(&shard_index) {
                    if let Some(entry) = guard.peek(key) {
                        if !entry.is_expired() {
                            match &entry.data {
                                DataValue::String(s) => RespValue::BulkString(s.clone()),
                                _ => RespValue::Null,
                            }
                        } else {
                            RespValue::Null
                        }
                    } else {
                        RespValue::Null
                    }
                } else {
                    RespValue::Null
                };
                responses.push(value);
            }
        } else {
            return Err(SpinelDBError::Internal(
                "MGET requires multi-shard lock".into(),
            ));
        }
        Ok((RespValue::Array(responses), WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for MGet {
    fn name(&self) -> &'static str {
        "mget"
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
        -1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_mget_parse_single_key() {
        let c = MGet::parse(&[bs("key")]).unwrap();
        assert_eq!(c.keys.len(), 1);
        assert_eq!(c.keys[0], Bytes::from_static(b"key"));
    }

    #[test]
    fn test_mget_parse_multiple_keys() {
        let c = MGet::parse(&[bs("a"), bs("b"), bs("c")]).unwrap();
        assert_eq!(c.keys.len(), 3);
    }

    #[test]
    fn test_mget_parse_empty_is_error() {
        let r = MGet::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_mget_parse_non_bulk_key_is_error() {
        let r = MGet::parse(&[bs("a"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_mget_to_resp_args() {
        let c = MGet {
            keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"a"));
    }

    #[test]
    fn test_mget_spec() {
        let c = MGet { keys: vec![] };
        assert_eq!(c.name(), "mget");
        assert_eq!(c.arity(), -2);
        assert!(c.flags().contains(CommandFlags::READONLY));
        assert_eq!(c.first_key(), 1);
    }
}
