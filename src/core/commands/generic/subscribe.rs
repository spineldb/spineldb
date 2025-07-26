// src/core/commands/generic/subscribe.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Subscribe {
    pub channels: Vec<Bytes>,
}
impl ParseCommand for Subscribe {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SUBSCRIBE".to_string()));
        }
        let channels = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Subscribe { channels })
    }
}
#[async_trait]
impl ExecutableCommand for Subscribe {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // This command is handled by the connection router to switch to Pub/Sub mode.
        Err(SpinelDBError::Internal(
            "SUBSCRIBE command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Subscribe {
    fn name(&self) -> &'static str {
        "subscribe"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::PUBSUB | CommandFlags::NO_PROPAGATE
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
        self.channels.clone()
    }
}
