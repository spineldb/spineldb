// src/core/commands/generic/time.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};

use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct Time;

impl ParseCommand for Time {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if !args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("TIME".to_string()));
        }
        Ok(Time)
    }
}

#[async_trait]
impl ExecutableCommand for Time {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let now = SystemTime::now();
        let duration_since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

        let seconds = duration_since_epoch.as_secs();
        let microseconds = duration_since_epoch.subsec_micros();

        let response = RespValue::Array(vec![
            RespValue::BulkString(seconds.to_string().into()),
            RespValue::BulkString(microseconds.to_string().into()),
        ]);

        Ok((response, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Time {
    fn name(&self) -> &'static str {
        "time"
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
