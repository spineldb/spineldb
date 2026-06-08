// src/core/commands/generic/punsubscribe.rs

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
pub struct PUnsubscribe {
    pub patterns: Vec<Bytes>,
}

impl ParseCommand for PUnsubscribe {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let patterns = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(PUnsubscribe { patterns })
    }
}

#[async_trait]
impl ExecutableCommand for PUnsubscribe {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "PUNSUBSCRIBE command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for PUnsubscribe {
    fn name(&self) -> &'static str {
        "punsubscribe"
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
        self.patterns.clone()
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
    fn test_parse_empty_no_patterns() {
        let cmd = PUnsubscribe::parse(&[]).unwrap();
        assert!(cmd.patterns.is_empty());
    }

    #[test]
    fn test_parse_single_pattern() {
        let cmd = PUnsubscribe::parse(&[bulk("news.*")]).unwrap();
        assert_eq!(cmd.patterns, vec![Bytes::from_static(b"news.*")]);
    }

    #[test]
    fn test_parse_multiple_patterns() {
        let cmd = PUnsubscribe::parse(&[bulk("a.*"), bulk("b.*")]).unwrap();
        assert_eq!(cmd.patterns.len(), 2);
    }

    #[test]
    fn test_command_name() {
        assert_eq!(PUnsubscribe::default().name(), "punsubscribe");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(PUnsubscribe::default().arity(), -1);
    }

    #[test]
    fn test_command_flags() {
        let flags = PUnsubscribe::default().flags();
        assert!(flags.contains(CommandFlags::PUBSUB));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(PUnsubscribe::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = PUnsubscribe::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = PUnsubscribe {
            patterns: vec![Bytes::from_static(b"ch.*")],
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"ch.*")]);
    }
}
