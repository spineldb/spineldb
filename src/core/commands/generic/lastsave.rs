// src/core/commands/generic/lastsave.rs

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

#[derive(Debug, Clone, Default)]
pub struct LastSave;

impl ParseCommand for LastSave {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "LASTSAVE")?;
        Ok(LastSave)
    }
}

#[async_trait]
impl ExecutableCommand for LastSave {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let last_save_success_time_guard =
            ctx.state.persistence.last_save_success_time.lock().await;
        let last_save_unix_ts = if let Some(instant) = *last_save_success_time_guard {
            let duration_since_save = instant.elapsed().as_secs();
            chrono::Utc::now().timestamp() - duration_since_save as i64
        } else {
            0 // Return 0 if no successful save has occurred yet, similar to SpinelDB.
        };

        Ok((
            RespValue::Integer(last_save_unix_ts),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl CommandSpec for LastSave {
    fn name(&self) -> &'static str {
        "lastsave"
    }
    fn arity(&self) -> i64 {
        1
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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
