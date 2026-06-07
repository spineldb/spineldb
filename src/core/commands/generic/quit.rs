// src/core/commands/generic/quit.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::validate_arg_count;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Quit;

impl ParseCommand for Quit {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "QUIT")?;
        Ok(Quit)
    }
}

// QUIT ditangani khusus oleh connection handler, jadi tidak perlu dieksekusi di sini.
#[async_trait]
impl ExecutableCommand for Quit {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "QUIT command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for Quit {
    fn name(&self) -> &'static str {
        "quit"
    }
    fn arity(&self) -> i64 {
        1
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
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_quit_parses_with_no_args() {
        assert!(Quit::parse(&[]).is_ok());
    }

    #[test]
    fn test_quit_with_any_args_is_error() {
        let r = Quit::parse(&[bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_quit_to_resp_args_is_empty() {
        assert!(Quit.to_resp_args().is_empty());
    }
}
