// src/core/commands/generic/bgsave.rs

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
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Default)]
pub struct BgSave;
impl ParseCommand for BgSave {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "BGSAVE")?;
        Ok(BgSave)
    }
}
#[async_trait]
impl ExecutableCommand for BgSave {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if ctx
            .state
            .persistence
            .aof_rewrite_state
            .lock()
            .await
            .is_in_progress
        {
            return Err(SpinelDBError::InvalidState(
                "ERR A background AOF rewrite is already in progress".into(),
            ));
        }

        if ctx
            .state
            .persistence
            .is_saving_spldb
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SpinelDBError::InvalidState(
                "ERR Background SPLDB save already in progress".into(),
            ));
        }

        let state_clone = ctx.state.clone();
        let handle: JoinHandle<()> = tokio::spawn(async move {
            if let Err(e) =
                crate::core::persistence::spldb_saver::SpldbSaverTask::perform_save_logic(
                    &state_clone,
                )
                .await
            {
                tracing::error!("Background SPLDB save failed: {}", e);
            } else {
                tracing::info!("Background SPLDB save completed successfully.");
            }
            state_clone
                .persistence
                .is_saving_spldb
                .store(false, Ordering::SeqCst);
            *state_clone.persistence.bgsave_handle.lock().await = None;
        });

        *ctx.state.persistence.bgsave_handle.lock().await = Some(handle);

        Ok((
            RespValue::SimpleString("Background saving started".to_string()),
            WriteOutcome::DidNotWrite,
        ))
    }
}
impl CommandSpec for BgSave {
    fn name(&self) -> &'static str {
        "bgsave"
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
        let cmd = BgSave::parse(&[]).unwrap();
        assert_eq!(cmd.name(), "bgsave");
    }

    #[test]
    fn test_parse_with_args_errors() {
        assert!(BgSave::parse(&[RespFrame::BulkString(bytes::Bytes::from("x"))]).is_err());
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(BgSave.arity(), 1);
    }

    #[test]
    fn test_command_flags() {
        let flags = BgSave.flags();
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(BgSave.get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = BgSave;
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args_empty() {
        assert!(BgSave.to_resp_args().is_empty());
    }
}
