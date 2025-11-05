// src/core/commands/generic/punsubscribe.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct PUnsubscribe {
    pub patterns: Vec<Bytes>,
}

impl ParseCommand for PUnsubscribe {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let patterns = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(PUnsubscribe { patterns })
    }
}

#[async_trait]
impl ExecutableCommand for PUnsubscribe {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "PUNSUBSCRIBE command should not be executed directly".into(),
        ))
    }
}

impl CommandSpec for PUnsubscribe {
    fn name(&self) -> &'static str {
        "punsubscribe"
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
        self.patterns.clone()
    }
}
