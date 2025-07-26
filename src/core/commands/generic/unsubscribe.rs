// src/core/commands/generic/unsubscribe.rs

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
pub struct Unsubscribe {
    pub channels: Vec<Bytes>,
}
impl ParseCommand for Unsubscribe {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let channels = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Unsubscribe { channels })
    }
}
#[async_trait]
impl ExecutableCommand for Unsubscribe {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // This command is handled by the connection router to manage Pub/Sub state.
        Err(SpinelDBError::Internal(
            "UNSUBSCRIBE command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Unsubscribe {
    fn name(&self) -> &'static str {
        "unsubscribe"
    }
    fn arity(&self) -> i64 {
        -1
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
