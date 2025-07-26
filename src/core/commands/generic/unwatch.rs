// src/core/commands/generic/unwatch.rs

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
pub struct Unwatch;

impl ParseCommand for Unwatch {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "UNWATCH")?;
        Ok(Unwatch)
    }
}

#[async_trait]
impl ExecutableCommand for Unwatch {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // This command is handled specially by the transaction router.
        Err(SpinelDBError::Internal(
            "UNWATCH command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for Unwatch {
    fn name(&self) -> &'static str {
        "unwatch"
    }
    fn arity(&self) -> i64 {
        1
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::TRANSACTION | CommandFlags::NO_PROPAGATE
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
