// src/core/commands/generic/save.rs

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

#[derive(Debug, Clone, Default)]
pub struct Save;

impl ParseCommand for Save {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "SAVE")?;
        Ok(Save)
    }
}
#[async_trait]
impl ExecutableCommand for Save {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if ctx
            .state
            .persistence
            .is_saving_spldb
            .load(Ordering::Relaxed)
            || ctx
                .state
                .persistence
                .aof_rewrite_state
                .lock()
                .await
                .is_in_progress
        {
            return Err(SpinelDBError::InvalidState(
                "ERR A background save is already in progress".into(),
            ));
        }

        // Panggil logika save secara langsung dan tunggu (await)
        let _ =
            crate::core::persistence::spldb_saver::SpldbSaverTask::perform_save_logic(&ctx.state)
                .await;

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::DidNotWrite,
        ))
    }
}
impl CommandSpec for Save {
    fn name(&self) -> &'static str {
        "save"
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
