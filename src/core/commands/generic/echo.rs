// src/core/commands/generic/echo.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Echo {
    pub message: Bytes,
}
impl ParseCommand for Echo {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "ECHO")?;
        Ok(Echo {
            message: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for Echo {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if !matches!(ctx.locks, ExecutionLocks::None) {
            return Err(SpinelDBError::Internal("ECHO should not have locks".into()));
        }
        Ok((
            RespValue::BulkString(self.message.clone()),
            WriteOutcome::DidNotWrite,
        ))
    }
}
impl CommandSpec for Echo {
    fn name(&self) -> &'static str {
        "echo"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::READONLY
    }
    fn first_key(&self) -> i64 {
        0
    }
    fn last_key(&self) -> i64 {
        0
    }
    fn step(&self) -> i64 {
        0
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.message.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_echo_parses_message() {
        let e = Echo::parse(&[bs("hello world")]).unwrap();
        assert_eq!(e.message, Bytes::from_static(b"hello world"));
    }

    #[test]
    fn test_echo_with_no_args_is_error() {
        let r = Echo::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_echo_with_too_many_args_is_error() {
        let r = Echo::parse(&[bs("a"), bs("b")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_echo_with_non_bulk_is_wrong_type() {
        let r = Echo::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_echo_with_empty_string_is_ok() {
        let e = Echo::parse(&[bs("")]).unwrap();
        assert!(e.message.is_empty());
    }

    #[test]
    fn test_echo_to_resp_args_round_trips() {
        let e = Echo::parse(&[bs("payload")]).unwrap();
        let args = e.to_resp_args();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], Bytes::from_static(b"payload"));
    }
}
