// src/core/commands/generic/ping.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Ping {
    pub message: Option<Bytes>,
}
impl ParseCommand for Ping {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        match args.len() {
            0 => Ok(Ping { message: None }),
            1 => Ok(Ping {
                message: Some(extract_bytes(&args[0])?),
            }),
            _ => Err(SpinelDBError::WrongArgumentCount("PING".to_string())),
        }
    }
}
#[async_trait]
impl ExecutableCommand for Ping {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if !matches!(ctx.locks, ExecutionLocks::None) {
            return Err(SpinelDBError::Internal("PING should not have locks".into()));
        }
        let resp = match &self.message {
            Some(msg) => RespValue::BulkString(msg.clone()),
            None => RespValue::SimpleString("PONG".into()),
        };
        Ok((resp, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for Ping {
    fn name(&self) -> &'static str {
        "ping"
    }
    fn arity(&self) -> i64 {
        -1
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
        self.message.clone().map_or(vec![], |msg| vec![msg])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_ping_no_args_yields_no_message() {
        let p = Ping::parse(&[]).unwrap();
        assert!(p.message.is_none());
    }

    #[test]
    fn test_ping_with_message_captures_payload() {
        let p = Ping::parse(&[bs("hello")]).unwrap();
        assert_eq!(p.message.as_deref(), Some(b"hello".as_ref()));
    }

    #[test]
    fn test_ping_with_too_many_args_is_error() {
        let r = Ping::parse(&[bs("a"), bs("b")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_ping_with_non_bulk_message_is_wrong_type() {
        let r = Ping::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_ping_to_resp_args_with_no_message_is_empty() {
        let p = Ping::default();
        assert!(p.to_resp_args().is_empty());
    }

    #[test]
    fn test_ping_to_resp_args_with_message_round_trips() {
        let p = Ping::parse(&[bs("hi")]).unwrap();
        let args = p.to_resp_args();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], Bytes::from_static(b"hi"));
    }
}
