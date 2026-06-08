// src/core/commands/generic/psubscribe.rs

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
pub struct PSubscribe {
    pub patterns: Vec<Bytes>,
}

impl ParseCommand for PSubscribe {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("PSUBSCRIBE".to_string()));
        }
        let patterns = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(PSubscribe { patterns })
    }
}

#[async_trait]
impl ExecutableCommand for PSubscribe {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "PSUBSCRIBE command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for PSubscribe {
    fn name(&self) -> &'static str {
        "psubscribe"
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
    fn test_parse_empty_errors() {
        assert!(PSubscribe::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_single_pattern() {
        let cmd = PSubscribe::parse(&[bulk("news.*")]).unwrap();
        assert_eq!(cmd.patterns, vec![Bytes::from_static(b"news.*")]);
    }

    #[test]
    fn test_parse_multiple_patterns() {
        let cmd = PSubscribe::parse(&[bulk("a.*"), bulk("b.*")]).unwrap();
        assert_eq!(cmd.patterns.len(), 2);
    }

    #[test]
    fn test_command_name() {
        assert_eq!(PSubscribe::default().name(), "psubscribe");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(PSubscribe::default().arity(), -2);
    }

    #[test]
    fn test_command_flags() {
        let flags = PSubscribe::default().flags();
        assert!(flags.contains(CommandFlags::PUBSUB));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(PSubscribe::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = PSubscribe::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args() {
        let cmd = PSubscribe {
            patterns: vec![Bytes::from_static(b"ch.*")],
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"ch.*")]);
    }
}
