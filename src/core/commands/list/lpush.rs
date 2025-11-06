// src/core/commands/list/lpush.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::commands::list::logic::list_push_logic;
use crate::core::database::{ExecutionContext, PushDirection};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct LPush {
    pub key: Bytes,
    pub values: Vec<Bytes>,
}

impl ParseCommand for LPush {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, values) = parse_key_and_values(args, 2, "LPUSH")?;
        Ok(LPush { key, values })
    }
}

#[async_trait]
impl ExecutableCommand for LPush {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        list_push_logic(ctx, &self.key, &self.values, PushDirection::Left).await
    }
}

impl CommandSpec for LPush {
    fn name(&self) -> &'static str {
        "lpush"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone()];
        args.extend(self.values.clone());
        args
    }
}
