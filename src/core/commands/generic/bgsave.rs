// src/core/commands/generic/bgsave.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::validate_arg_count;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

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
        // SpinelDB does not allow BGSAVE if BGREWRITEAOF is in progress.
        // This is a safety measure to avoid excessive disk I/O contention.
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

        // Atomically check and set the `is_saving_spldb` flag to prevent concurrent saves.
        // `compare_exchange` ensures that only one task can start the save process.
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

        // Spawn the save task in the background to avoid blocking the client.
        let state_clone = ctx.state.clone();
        tokio::spawn(async move {
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
            // Ensure the flag is reset regardless of the outcome.
            state_clone
                .persistence
                .is_saving_spldb
                .store(false, Ordering::SeqCst);
        });

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
