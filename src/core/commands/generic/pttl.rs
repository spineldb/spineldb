// src/core/commands/generic/pttl.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Pttl {
    pub key: Bytes,
}
impl ParseCommand for Pttl {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "PTTL")?;
        Ok(Pttl {
            key: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for Pttl {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let shard_cache_guard = match &mut ctx.locks {
            ExecutionLocks::Single { guard, .. } => guard,
            ExecutionLocks::Multi { guards } => {
                let shard_index = ctx.db.get_shard_index(&self.key);
                guards.get_mut(&shard_index).ok_or_else(|| {
                    SpinelDBError::Internal("Mismatched lock in multi-key command for PTTL".into())
                })?
            }
            _ => return Err(SpinelDBError::Internal("PTTL requires a shard lock".into())),
        };

        let result = if let Some(entry) = shard_cache_guard.get(&self.key) {
            if entry.is_expired() {
                -2 // Key ada tapi expired
            } else {
                // Key ada, kembalikan TTL atau -1 jika tidak ada
                entry.remaining_ttl_ms().unwrap_or(-1)
            }
        } else {
            -2 // Key tidak ada
        };
        Ok((RespValue::Integer(result), WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for Pttl {
    fn name(&self) -> &'static str {
        "pttl"
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
