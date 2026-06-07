// src/core/commands/list/lrange.rs

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
pub struct LRange {
    pub key: Bytes,
    pub start: i64,
    pub stop: i64,
}
impl ParseCommand for LRange {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "LRANGE")?;
        Ok(LRange {
            key: extract_bytes(&args[0])?,
            start: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
            stop: extract_string(&args[2])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for LRange {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Use consistent helper and handle passive expiration.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let resp = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                RespValue::Array(vec![])
            } else if let DataValue::List(list) = &entry.data {
                let len = list.len() as i64;
                if len == 0 {
                    return Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite));
                }
                let start = if self.start < 0 {
                    len + self.start
                } else {
                    self.start
                };
                let stop = if self.stop < 0 {
                    len + self.stop
                } else {
                    self.stop
                };
                let start = start.max(0) as usize;
                let stop = stop.min(len - 1) as usize;
                if start > stop || start as i64 >= len {
                    return Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite));
                }
                let values = list
                    .iter()
                    .skip(start)
                    .take(stop - start + 1)
                    .cloned()
                    .map(RespValue::BulkString)
                    .collect();
                RespValue::Array(values)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            RespValue::Array(vec![])
        };
        Ok((resp, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for LRange {
    fn name(&self) -> &'static str {
        "lrange"
    }
    fn arity(&self) -> i64 {
        4
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
        vec![
            self.key.clone(),
            self.start.to_string().into(),
            self.stop.to_string().into(),
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
    fn test_lrange_parses_key_start_stop() {
        let c = LRange::parse(&[bs("k"), bs("0"), bs("10")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.start, 0);
        assert_eq!(c.stop, 10);
    }

    #[test]
    fn test_lrange_parses_negative_indices() {
        let c = LRange::parse(&[bs("k"), bs("-5"), bs("-1")]).unwrap();
        assert_eq!(c.start, -5);
        assert_eq!(c.stop, -1);
    }

    #[test]
    fn test_lrange_with_too_few_args_is_error() {
        let r = LRange::parse(&[bs("k"), bs("0")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lrange_with_too_many_args_is_error() {
        let r = LRange::parse(&[bs("k"), bs("0"), bs("1"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lrange_with_non_integer_start_is_error() {
        let r = LRange::parse(&[bs("k"), bs("start"), bs("10")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_lrange_with_non_integer_stop_is_error() {
        let r = LRange::parse(&[bs("k"), bs("0"), bs("end")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_lrange_to_resp_args_round_trips() {
        let c = LRange::parse(&[bs("k"), bs("0"), bs("-1")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"0"));
        assert_eq!(args[2], Bytes::from_static(b"-1"));
    }
}
