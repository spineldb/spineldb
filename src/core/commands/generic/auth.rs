// src/core/commands/generic/auth.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Auth {
    pub password: String,
}

impl ParseCommand for Auth {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("AUTH".to_string()));
        }

        let password = extract_string(&args[0])?;

        Ok(Auth { password })
    }
}

#[async_trait]
impl ExecutableCommand for Auth {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "AUTH command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for Auth {
    fn name(&self) -> &'static str {
        "auth"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE
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
        vec![self.password.clone().into()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_auth_parses_password() {
        let a = Auth::parse(&[bs("secret")]).unwrap();
        assert_eq!(a.password, "secret");
    }

    #[test]
    fn test_auth_with_no_args_is_error() {
        let r = Auth::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_auth_with_too_many_args_is_error() {
        let r = Auth::parse(&[bs("u"), bs("p")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_auth_with_non_bulk_is_wrong_type() {
        let r = Auth::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_auth_with_empty_password_is_ok() {
        let a = Auth::parse(&[bs("")]).unwrap();
        assert_eq!(a.password, "");
    }

    #[test]
    fn test_auth_to_resp_args_round_trips() {
        let a = Auth::parse(&[bs("hunter2")]).unwrap();
        let args = a.to_resp_args();
        assert_eq!(args, vec![Bytes::from_static(b"hunter2")]);
    }
}
