// src/core/commands/string/bitcount.rs

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

#[derive(Debug, Clone, Default)]
pub struct BitCount {
    pub key: Bytes,
    pub range: Option<(i64, i64)>,
}

impl ParseCommand for BitCount {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 3 {
            return Err(SpinelDBError::WrongArgumentCount("BITCOUNT".to_string()));
        }

        let mut cmd = BitCount {
            key: extract_bytes(&args[0])?,
            ..Default::default()
        };

        if args.len() == 3 {
            let start = extract_string(&args[1])?
                .parse::<i64>()
                .map_err(|_| SpinelDBError::NotAnInteger)?;
            let end = extract_string(&args[2])?
                .parse::<i64>()
                .map_err(|_| SpinelDBError::NotAnInteger)?;
            cmd.range = Some((start, end));
        } else if args.len() == 2 {
            return Err(SpinelDBError::SyntaxError);
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for BitCount {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Use consistent helper and handle passive expiration.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let count = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                0
            } else if let DataValue::String(s) = &entry.data {
                let len = s.len() as i64;
                let (start, end) = self.range.map_or((0, len - 1), |(s, e)| {
                    let start = if s < 0 { len + s } else { s };
                    let end = if e < 0 { len + e } else { e };
                    (start.max(0), end.min(len - 1))
                });

                if start > end {
                    0
                } else {
                    s[start as usize..=end as usize]
                        .iter()
                        .map(|byte| byte.count_ones() as i64)
                        .sum()
                }
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            0
        };

        Ok((RespValue::Integer(count), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for BitCount {
    fn name(&self) -> &'static str {
        "bitcount"
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
        if let Some((start, end)) = self.range {
            args.push(start.to_string().into());
            args.push(end.to_string().into());
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
    fn test_bitcount_parse_basic() {
        let c = BitCount::parse(&[bs("mykey")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"mykey"));
        assert!(c.range.is_none());
    }

    #[test]
    fn test_bitcount_parse_with_range() {
        let c = BitCount::parse(&[bs("mykey"), bs("0"), bs("10")]).unwrap();
        assert_eq!(c.range, Some((0, 10)));
    }

    #[test]
    fn test_bitcount_parse_with_negative_range() {
        let c = BitCount::parse(&[bs("mykey"), bs("-5"), bs("-1")]).unwrap();
        assert_eq!(c.range, Some((-5, -1)));
    }

    #[test]
    fn test_bitcount_parse_empty_args_is_error() {
        let r = BitCount::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bitcount_parse_too_many_args_is_error() {
        let r = BitCount::parse(&[bs("k"), bs("a"), bs("b"), bs("c")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bitcount_parse_two_args_is_error() {
        let r = BitCount::parse(&[bs("k"), bs("a")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_bitcount_parse_non_bulk_string_is_error() {
        let r = BitCount::parse(&[RespFrame::Integer(123)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_bitcount_to_resp_args_basic() {
        let c = BitCount {
            key: Bytes::from_static(b"key"),
            ..Default::default()
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], Bytes::from_static(b"key"));
    }

    #[test]
    fn test_bitcount_to_resp_args_with_range() {
        let c = BitCount {
            key: Bytes::from_static(b"key"),
            range: Some((5, 10)),
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[1], Bytes::from_static(b"5"));
        assert_eq!(args[2], Bytes::from_static(b"10"));
    }

    #[test]
    fn test_bitcount_spec() {
        let c = BitCount {
            key: Bytes::from_static(b"k"),
            ..Default::default()
        };
        assert_eq!(c.name(), "bitcount");
        assert_eq!(c.arity(), -2);
        assert!(c.flags().contains(CommandFlags::READONLY));
        assert_eq!(c.first_key(), 1);
        assert_eq!(c.step(), 1);
    }
}
