// src/core/commands/generic/echo.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Echo {
    pub message: Bytes,
}
impl ParseCommand for Echo {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "ECHO")?;
        Ok(Echo {
            message: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for Echo {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if !matches!(ctx.locks, ExecutionLocks::None) {
            return Err(SpinelDBError::Internal("ECHO should not have locks".into()));
        }
        Ok((
            RespValue::BulkString(self.message.clone()),
            WriteOutcome::DidNotWrite,
        ))
    }
}
impl CommandSpec for Echo {
    fn name(&self) -> &'static str {
        "echo"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::READONLY
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
        vec![self.message.clone()]
    }
}
