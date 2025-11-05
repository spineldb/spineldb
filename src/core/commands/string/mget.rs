// src/core/commands/string/mget.rs
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct MGet {
    pub keys: Vec<Bytes>,
}
impl ParseCommand for MGet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("MGET".to_string()));
        }
        let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(MGet { keys })
    }
}
#[async_trait]
impl ExecutableCommand for MGet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut responses = Vec::with_capacity(self.keys.len());
        if let ExecutionLocks::Multi { guards } = &ctx.locks {
            for key in &self.keys {
                let shard_index = ctx.db.get_shard_index(key);
                let value = if let Some(guard) = guards.get(&shard_index) {
                    if let Some(entry) = guard.peek(key) {
                        if !entry.is_expired() {
                            match &entry.data {
                                DataValue::String(s) => RespValue::BulkString(s.clone()),
                                _ => RespValue::Null,
                            }
                        } else {
                            RespValue::Null
                        }
                    } else {
                        RespValue::Null
                    }
                } else {
                    RespValue::Null
                };
                responses.push(value);
            }
        } else {
            return Err(SpinelDBError::Internal(
                "MGET requires multi-shard lock".into(),
            ));
        }
        Ok((RespValue::Array(responses), WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for MGet {
    fn name(&self) -> &'static str {
        "mget"
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
