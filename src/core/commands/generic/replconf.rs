// src/core/commands/generic/replconf.rs

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
pub struct Replconf {
    pub args: Vec<String>,
}
impl ParseCommand for Replconf {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("REPLCONF".to_string()));
        }
        let str_args = args
            .iter()
            .map(extract_string)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Replconf { args: str_args })
    }
}
#[async_trait]
impl ExecutableCommand for Replconf {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "REPLCONF command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Replconf {
    fn name(&self) -> &'static str {
        "replconf"
    }
    fn arity(&self) -> i64 {
        -1
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
        self.args.iter().map(|s| s.clone().into()).collect()
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
        assert!(Replconf::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_single_arg() {
        let cmd = Replconf::parse(&[bulk("GETACK")]).unwrap();
        assert_eq!(cmd.args, vec!["GETACK"]);
    }

    #[test]
    fn test_parse_multiple_args() {
        let cmd = Replconf::parse(&[bulk("ACK"), bulk("0")]).unwrap();
        assert_eq!(cmd.args, vec!["ACK", "0"]);
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Replconf::default().name(), "replconf");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Replconf::default().arity(), -1);
    }

    #[test]
    fn test_command_flags() {
        let flags = Replconf::default().flags();
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Replconf::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Replconf::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = Replconf {
            args: vec!["ACK".into(), "100".into()],
        };
        assert_eq!(
            cmd.to_resp_args(),
            vec![Bytes::from_static(b"ACK"), Bytes::from_static(b"100")]
        );
    }

    #[test]
    fn test_to_resp_args_empty() {
        let cmd = Replconf { args: vec![] };
        assert!(cmd.to_resp_args().is_empty());
    }
}
