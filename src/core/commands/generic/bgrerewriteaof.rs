// src/core/commands/generic/bgrerewriteaof.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::validate_arg_count;
use crate::core::database::ExecutionContext;
use crate::core::persistence::rewrite_aof;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Default)]
pub struct BgRewriteAof;
impl ParseCommand for BgRewriteAof {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "BGREWRITEAOF")?;
        Ok(BgRewriteAof)
    }
}
#[async_trait]
impl ExecutableCommand for BgRewriteAof {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let is_in_progress = ctx
            .state
            .persistence
            .aof_rewrite_state
            .try_lock()
            .map(|guard| guard.is_in_progress)
            .unwrap_or(true);

        if is_in_progress {
            return Err(SpinelDBError::InvalidState(
                "ERR Background AOF rewrite already in progress".to_string(),
            ));
        }

        let state_clone = ctx.state.clone();
        let handle: JoinHandle<()> = tokio::spawn(async move {
            rewrite_aof(state_clone).await;
        });
        *ctx.state.persistence.aof_rewrite_handle.lock().await = Some(handle);
        Ok((
            RespValue::SimpleString("Background AOF rewrite started".into()),
            WriteOutcome::DidNotWrite,
        ))
    }
}
impl CommandSpec for BgRewriteAof {
    fn name(&self) -> &'static str {
        "bgrewriteaof"
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
    use crate::core::commands::command_spec::CommandSpec;
    use crate::core::commands::command_trait::ParseCommand;
    use crate::core::protocol::RespFrame;

    #[test]
    fn test_parse_no_args() {
        let cmd = BgRewriteAof::parse(&[]).unwrap();
        assert_eq!(cmd.name(), "bgrewriteaof");
    }

    #[test]
    fn test_parse_with_args_errors() {
        assert!(BgRewriteAof::parse(&[RespFrame::BulkString(bytes::Bytes::from("x"))]).is_err());
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(BgRewriteAof.arity(), 1);
    }

    #[test]
    fn test_command_flags() {
        let flags = BgRewriteAof.flags();
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(BgRewriteAof.get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = BgRewriteAof;
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args_empty() {
        assert!(BgRewriteAof.to_resp_args().is_empty());
    }
}
