// src/core/commands/generic/ping.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Ping {
    pub message: Option<Bytes>,
}
impl ParseCommand for Ping {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        match args.len() {
            0 => Ok(Ping { message: None }),
            1 => Ok(Ping {
                message: Some(extract_bytes(&args[0])?),
            }),
            _ => Err(SpinelDBError::WrongArgumentCount("PING".to_string())),
        }
    }
}
#[async_trait]
impl ExecutableCommand for Ping {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if !matches!(ctx.locks, ExecutionLocks::None) {
            return Err(SpinelDBError::Internal("PING should not have locks".into()));
        }
        let resp = match &self.message {
            Some(msg) => RespValue::BulkString(msg.clone()),
            None => RespValue::SimpleString("PONG".into()),
        };
        Ok((resp, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for Ping {
    fn name(&self) -> &'static str {
        "ping"
    }
    fn arity(&self) -> i64 {
        -1
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
        self.message.clone().map_or(vec![], |msg| vec![msg])
    }
}
