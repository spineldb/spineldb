// src/core/commands/string/getrange.rs

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
pub struct GetRange {
    pub key: Bytes,
    pub start: i64,
    pub end: i64,
}

impl ParseCommand for GetRange {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "GETRANGE")?;
        Ok(GetRange {
            key: extract_bytes(&args[0])?,
            start: extract_string(&args[1])?.parse()?,
            end: extract_string(&args[2])?.parse()?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for GetRange {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        if let Some(entry) = guard.get(&self.key) {
            if entry.is_expired() {
                return Ok((
                    RespValue::BulkString(Bytes::new()),
                    WriteOutcome::DidNotWrite,
                ));
            }

            if let DataValue::String(s) = &entry.data {
                let len = s.len() as i64;
                let start = if self.start < 0 {
                    len + self.start
                } else {
                    self.start
                }
                .max(0) as usize;
                let end = if self.end < 0 {
                    len + self.end
                } else {
                    self.end
                }
                .max(0) as usize;

                if start > end || start >= s.len() {
                    return Ok((
                        RespValue::BulkString(Bytes::new()),
                        WriteOutcome::DidNotWrite,
                    ));
                }

                let end = end.min(s.len() - 1);
                let result = s.slice(start..=end);
                return Ok((RespValue::BulkString(result), WriteOutcome::DidNotWrite));
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }

        Ok((
            RespValue::BulkString(Bytes::new()),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl CommandSpec for GetRange {
    fn name(&self) -> &'static str {
        "getrange"
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
            self.end.to_string().into(),
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
    fn test_getrange_parses_key_start_end() {
        let g = GetRange::parse(&[bs("k"), bs("0"), bs("5")]).unwrap();
        assert_eq!(g.key, Bytes::from_static(b"k"));
        assert_eq!(g.start, 0);
        assert_eq!(g.end, 5);
    }

    #[test]
    fn test_getrange_parses_negative_indices() {
        let g = GetRange::parse(&[bs("k"), bs("-10"), bs("-1")]).unwrap();
        assert_eq!(g.start, -10);
        assert_eq!(g.end, -1);
    }

    #[test]
    fn test_getrange_with_too_few_args_is_error() {
        let r = GetRange::parse(&[bs("k"), bs("0")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_getrange_with_too_many_args_is_error() {
        let r = GetRange::parse(&[bs("k"), bs("0"), bs("5"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_getrange_with_non_integer_start_is_error() {
        let r = GetRange::parse(&[bs("k"), bs("zero"), bs("5")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_getrange_with_non_integer_end_is_error() {
        let r = GetRange::parse(&[bs("k"), bs("0"), bs("end")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_getrange_to_resp_args_round_trips() {
        let g = GetRange::parse(&[bs("k"), bs("0"), bs("5")]).unwrap();
        let args = g.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"0"));
        assert_eq!(args[2], Bytes::from_static(b"5"));
    }
}
