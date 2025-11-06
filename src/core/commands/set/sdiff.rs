// src/core/commands/set/sdiff.rs

use super::set_ops_logic::execute_sdiff;
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
pub struct Sdiff {
    pub keys: Vec<Bytes>,
}

impl ParseCommand for Sdiff {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SDIFF".to_string()));
        }
        let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(Sdiff { keys })
    }
}

#[async_trait]
impl ExecutableCommand for Sdiff {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Propagate WRONGTYPE error from execute_sdiff if any key is not a set.
        let diff_set = execute_sdiff(&self.keys, ctx).await?;

        let result = diff_set.into_iter().map(RespValue::BulkString).collect();
        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Sdiff {
    fn name(&self) -> &'static str {
        "sdiff"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
}
