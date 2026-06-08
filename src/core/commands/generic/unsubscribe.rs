// src/core/commands/generic/unsubscribe.rs

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
pub struct Unsubscribe {
    pub channels: Vec<Bytes>,
}
impl ParseCommand for Unsubscribe {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let channels = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Unsubscribe { channels })
    }
}
#[async_trait]
impl ExecutableCommand for Unsubscribe {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // This command is handled by the connection router to manage Pub/Sub state.
        Err(SpinelDBError::Internal(
            "UNSUBSCRIBE command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Unsubscribe {
    fn name(&self) -> &'static str {
        "unsubscribe"
    }
    fn arity(&self) -> i64 {
        -1
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
    fn test_parse_empty_no_channels() {
        let cmd = Unsubscribe::parse(&[]).unwrap();
        assert!(cmd.channels.is_empty());
    }

    #[test]
    fn test_parse_single_channel() {
        let cmd = Unsubscribe::parse(&[bulk("news")]).unwrap();
        assert_eq!(cmd.channels, vec![Bytes::from_static(b"news")]);
    }

    #[test]
    fn test_parse_multiple_channels() {
        let cmd = Unsubscribe::parse(&[bulk("ch1"), bulk("ch2")]).unwrap();
        assert_eq!(cmd.channels.len(), 2);
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Unsubscribe::default().name(), "unsubscribe");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Unsubscribe::default().arity(), -1);
    }

    #[test]
    fn test_command_flags() {
        let flags = Unsubscribe::default().flags();
        assert!(flags.contains(CommandFlags::PUBSUB));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Unsubscribe::default().get_keys().is_empty());
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = Unsubscribe {
            channels: vec![Bytes::from_static(b"a")],
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"a")]);
    }
}
