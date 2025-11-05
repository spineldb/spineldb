// src/core/commands/streams/xlen.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct XLen {
    key: Bytes,
}

impl ParseCommand for XLen {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("XLEN".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        Ok(XLen { key })
    }
}

#[async_trait]
impl ExecutableCommand for XLen {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        if let Some(entry) = guard.peek(&self.key) {
            if entry.is_expired() {
                return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
            }

            if let DataValue::Stream(stream) = &entry.data {
                Ok((
                    RespValue::Integer(stream.length as i64),
                    WriteOutcome::DidNotWrite,
                ))
            } else {
                Err(SpinelDBError::WrongType)
            }
        } else {
            Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
        }
    }
}

impl CommandSpec for XLen {
    fn name(&self) -> &'static str {
        "xlen"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone()]
    }
}
