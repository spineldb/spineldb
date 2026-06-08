// src/core/commands/generic/subscribe.rs

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
pub struct Subscribe {
    pub channels: Vec<Bytes>,
}
impl ParseCommand for Subscribe {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SUBSCRIBE".to_string()));
        }
        let channels = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Subscribe { channels })
    }
}
#[async_trait]
impl ExecutableCommand for Subscribe {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // This command is handled by the connection router to switch to Pub/Sub mode.
        Err(SpinelDBError::Internal(
            "SUBSCRIBE command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Subscribe {
    fn name(&self) -> &'static str {
        "subscribe"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::PUBSUB | CommandFlags::NO_PROPAGATE
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
        self.channels.clone()
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
        assert!(Subscribe::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_single_channel() {
        let cmd = Subscribe::parse(&[bulk("news")]).unwrap();
        assert_eq!(cmd.channels, vec![Bytes::from_static(b"news")]);
    }

    #[test]
    fn test_parse_multiple_channels() {
        let cmd = Subscribe::parse(&[bulk("ch1"), bulk("ch2"), bulk("ch3")]).unwrap();
        assert_eq!(cmd.channels.len(), 3);
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Subscribe::default().name(), "subscribe");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Subscribe::default().arity(), -2);
    }

    #[test]
    fn test_command_flags() {
        let flags = Subscribe::default().flags();
        assert!(flags.contains(CommandFlags::PUBSUB));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Subscribe::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Subscribe::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = Subscribe {
            channels: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        };
        assert_eq!(
            cmd.to_resp_args(),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }
}
