// src/core/commands/set/sunion.rs

use super::set_ops_logic::execute_sunion;
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
pub struct SUnion {
    pub keys: Vec<Bytes>,
}

impl ParseCommand for SUnion {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SUNION".to_string()));
        }
        let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(SUnion { keys })
    }
}

#[async_trait]
impl ExecutableCommand for SUnion {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Propagate WRONGTYPE error from execute_sunion if any key is not a set.
        let union_set = execute_sunion(&self.keys, ctx).await?;

        let result = union_set.into_iter().map(RespValue::BulkString).collect();
        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for SUnion {
    fn name(&self) -> &'static str {
        "sunion"
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
