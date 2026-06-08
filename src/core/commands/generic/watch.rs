// src/core/commands/generic/watch.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Watch {
    pub keys: Vec<Bytes>,
}
impl ParseCommand for Watch {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("WATCH".to_string()));
        }
        let keys = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Watch { keys })
    }
}
#[async_trait]
impl ExecutableCommand for Watch {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // This command is handled specially by the transaction router.
        Err(SpinelDBError::Internal(
            "WATCH command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Watch {
    fn name(&self) -> &'static str {
        "watch"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::TRANSACTION | CommandFlags::NO_PROPAGATE | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
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
        assert!(Watch::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_single_key() {
        let cmd = Watch::parse(&[bulk("mykey")]).unwrap();
        assert_eq!(cmd.keys, vec![Bytes::from_static(b"mykey")]);
    }

    #[test]
    fn test_parse_multiple_keys() {
        let cmd = Watch::parse(&[bulk("k1"), bulk("k2"), bulk("k3")]).unwrap();
        assert_eq!(cmd.keys.len(), 3);
        assert_eq!(cmd.keys[0], Bytes::from_static(b"k1"));
        assert_eq!(cmd.keys[1], Bytes::from_static(b"k2"));
        assert_eq!(cmd.keys[2], Bytes::from_static(b"k3"));
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Watch::default().name(), "watch");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Watch::default().arity(), -2);
    }

    #[test]
    fn test_command_flags() {
        let flags = Watch::default().flags();
        assert!(flags.contains(CommandFlags::TRANSACTION));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
        assert!(flags.contains(CommandFlags::MOVABLEKEYS));
    }

    #[test]
    fn test_get_keys() {
        let cmd = Watch {
            keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        };
        assert_eq!(
            cmd.get_keys(),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Watch::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Watch::default();
        assert_eq!(cmd.first_key(), 1);
        assert_eq!(cmd.last_key(), -1);
        assert_eq!(cmd.step(), 1);
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = Watch {
            keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
        };
        assert_eq!(
            cmd.to_resp_args(),
            vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]
        );
    }

    #[test]
    fn test_to_resp_args_empty() {
        assert!(Watch::default().to_resp_args().is_empty());
    }
}
