// src/core/commands/generic/unwatch.rs

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
pub struct Unwatch;

impl ParseCommand for Unwatch {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "UNWATCH")?;
        Ok(Unwatch)
    }
}

#[async_trait]
impl ExecutableCommand for Unwatch {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // This command is handled specially by the transaction router.
        Err(SpinelDBError::Internal(
            "UNWATCH command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for Unwatch {
    fn name(&self) -> &'static str {
        "unwatch"
    }
    fn arity(&self) -> i64 {
        1
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::TRANSACTION | CommandFlags::NO_PROPAGATE
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
    use crate::core::commands::command_spec::CommandSpec;
    use crate::core::commands::command_trait::ParseCommand;
    use crate::core::protocol::RespFrame;

    #[test]
    fn test_parse_no_args() {
        let cmd = Unwatch::parse(&[]).unwrap();
        assert_eq!(cmd.name(), "unwatch");
    }

    #[test]
    fn test_parse_with_args_errors() {
        assert!(Unwatch::parse(&[RespFrame::BulkString(bytes::Bytes::from("x"))]).is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Unwatch.name(), "unwatch");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Unwatch.arity(), 1);
    }

    #[test]
    fn test_command_flags() {
        let flags = Unwatch.flags();
        assert!(flags.contains(CommandFlags::TRANSACTION));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Unwatch.get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        assert_eq!(Unwatch.first_key(), 0);
        assert_eq!(Unwatch.last_key(), 0);
        assert_eq!(Unwatch.step(), 0);
    }

    #[test]
    fn test_to_resp_args_empty() {
        assert!(Unwatch.to_resp_args().is_empty());
    }
}
