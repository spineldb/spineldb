// src/core/commands/generic/select.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Select {
    pub db_index: usize,
}

impl ParseCommand for Select {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "SELECT")?;
        let index = extract_string(&args[0])?
            .parse::<usize>()
            .map_err(|_| SpinelDBError::InvalidState("db index is not an integer".into()))?;
        Ok(Select { db_index: index })
    }
}

// SELECT adalah perintah khusus yang mengubah state koneksi, jadi eksekusinya
// ditangani langsung di `command_router` dan tidak akan pernah sampai di sini.
#[async_trait]
impl ExecutableCommand for Select {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "SELECT command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for Select {
    fn name(&self) -> &'static str {
        "select"
    }
    fn arity(&self) -> i64 {
        2
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
        vec![self.db_index.to_string().into()]
    }
}
