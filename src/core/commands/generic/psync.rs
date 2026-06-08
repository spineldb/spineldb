// src/core/commands/generic/psync.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Psync {
    pub replication_id: String,
    pub offset: String,
}
impl ParseCommand for Psync {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "PSYNC")?;
        Ok(Psync {
            replication_id: extract_string(&args[0])?,
            offset: extract_string(&args[1])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for Psync {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "PSYNC command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Psync {
    fn name(&self) -> &'static str {
        "psync"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![
            self.replication_id.clone().into(),
            self.offset.clone().into(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::commands::command_spec::CommandSpec;
    use crate::core::commands::command_trait::ParseCommand;
    use crate::core::protocol::RespFrame;

    fn bulk(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::from(s.to_owned()))
    }

    #[test]
    fn test_parse_too_few_args_errors() {
        assert!(Psync::parse(&[]).is_err());
        assert!(Psync::parse(&[bulk("abc")]).is_err());
    }

    #[test]
    fn test_parse_valid() {
        let cmd = Psync::parse(&[bulk("abc123"), bulk("0")]).unwrap();
        assert_eq!(cmd.replication_id, "abc123");
        assert_eq!(cmd.offset, "0");
    }

    #[test]
    fn test_parse_questionmark() {
        let cmd = Psync::parse(&[bulk("?"), bulk("-1")]).unwrap();
        assert_eq!(cmd.replication_id, "?");
        assert_eq!(cmd.offset, "-1");
    }

    #[test]
    fn test_parse_too_many_args_errors() {
        assert!(Psync::parse(&[bulk("a"), bulk("0"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Psync::default().name(), "psync");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Psync::default().arity(), 3);
    }

    #[test]
    fn test_command_flags() {
        let flags = Psync::default().flags();
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Psync::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Psync::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = Psync {
            replication_id: "abc".into(),
            offset: "100".into(),
        };
        assert_eq!(
            cmd.to_resp_args(),
            vec![Bytes::from_static(b"abc"), Bytes::from_static(b"100")]
        );
    }
}
