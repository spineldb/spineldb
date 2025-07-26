// src/core/commands/generic/dbsize.rs

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
pub struct DbSize;

impl ParseCommand for DbSize {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "DBSIZE")?;
        Ok(DbSize)
    }
}

#[async_trait]
impl ExecutableCommand for DbSize {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let total_count = ctx.db.get_key_count();

        Ok((
            RespValue::Integer(total_count as i64),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl CommandSpec for DbSize {
    fn name(&self) -> &'static str {
        "dbsize"
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
