// src/core/commands/generic/backup.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::persistence::spldb;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use tracing::info;

#[derive(Debug, Clone, Default)]
pub struct Backup {
    pub path: String,
}

impl ParseCommand for Backup {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "BACKUP")?;
        Ok(Backup {
            path: extract_string(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Backup {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match spldb::save(&ctx.state.dbs, &self.path).await {
            Ok(_) => {
                info!("Manual backup to '{}' completed successfully.", self.path);
                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            Err(e) => Err(SpinelDBError::Internal(format!(
                "Failed to save backup to '{}': {}",
                self.path, e
            ))),
        }
    }
}

impl CommandSpec for Backup {
    fn name(&self) -> &'static str {
        "backup"
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
        vec![self.path.clone().into()]
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
    fn test_parse_empty_errors() {
        assert!(Backup::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_valid() {
        let cmd = Backup::parse(&[bulk("/tmp/backup.spldb")]).unwrap();
        assert_eq!(cmd.path, "/tmp/backup.spldb");
    }

    #[test]
    fn test_parse_too_many_args_errors() {
        assert!(Backup::parse(&[bulk("a"), bulk("b")]).is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Backup::default().name(), "backup");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Backup::default().arity(), 2);
    }

    #[test]
    fn test_command_flags() {
        let flags = Backup::default().flags();
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Backup::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Backup::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = Backup {
            path: "/tmp/bak.spldb".into(),
        };
        assert_eq!(
            cmd.to_resp_args(),
            vec![Bytes::from_static(b"/tmp/bak.spldb")]
        );
    }
}
