// src/core/commands/string/decrby.rs
use super::incr::do_incr_decr_by;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct DecrBy {
    pub key: Bytes,
    pub decrement: i64,
}
impl ParseCommand for DecrBy {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "DECRBY")?;
        let decrement = extract_string(&args[1])?
            .parse::<i64>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;
        Ok(DecrBy {
            key: extract_bytes(&args[0])?,
            decrement,
        })
    }
}
#[async_trait]
impl ExecutableCommand for DecrBy {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        do_incr_decr_by(&self.key, -self.decrement, ctx).await
    }
}
impl CommandSpec for DecrBy {
    fn name(&self) -> &'static str {
        "decrby"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.key.clone(), self.decrement.to_string().into()]
    }
}
