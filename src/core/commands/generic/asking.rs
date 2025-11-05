// src/core/commands/generic/asking.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Asking;

impl ParseCommand for Asking {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if !args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "ASKING command".to_string(),
            ));
        }
        Ok(Asking)
    }
}

#[async_trait]
impl ExecutableCommand for Asking {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Logika perintah ini ditangani sepenuhnya di `command_router`.
        // Eksekusi langsung di sini tidak seharusnya terjadi.
        Err(SpinelDBError::Internal(
            "ASKING command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for Asking {
    fn name(&self) -> &'static str {
        "asking"
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
