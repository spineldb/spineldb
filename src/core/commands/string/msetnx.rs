// src/core/commands/string/msetnx.rs
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct MSetNx {
    pub pairs: Vec<(Bytes, Bytes)>,
}
impl ParseCommand for MSetNx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 || args.len() % 2 != 0 {
            return Err(SpinelDBError::WrongArgumentCount("MSETNX".to_string()));
        }
        let pairs = args
            .chunks_exact(2)
            .map(|chunk| -> Result<(Bytes, Bytes), SpinelDBError> {
                Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?))
            })
            .collect::<Result<_, _>>()?;
        Ok(MSetNx { pairs })
    }
}
#[async_trait]
impl ExecutableCommand for MSetNx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if let ExecutionLocks::Multi { guards } = &mut ctx.locks {
            for (key, _) in &self.pairs {
                let shard_index = ctx.db.get_shard_index(key);
                if let Some(guard) = guards.get(&shard_index) {
                    if guard.peek(key).is_some_and(|e| !e.is_expired()) {
                        return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
                    }
                }
            }

            let keys_modified = self.pairs.len() as u64;
            for (key, value) in &self.pairs {
                let shard_index = ctx.db.get_shard_index(key);
                if let Some(guard) = guards.get_mut(&shard_index) {
                    let new_stored_value = StoredValue::new(DataValue::String(value.clone()));
                    guard.put(key.clone(), new_stored_value);
                }
            }
            return Ok((RespValue::Integer(1), WriteOutcome::Write { keys_modified }));
        }
        Err(SpinelDBError::Internal(
            "MSETNX requires multi-shard lock".into(),
        ))
    }
}
impl CommandSpec for MSetNx {
    fn name(&self) -> &'static str {
        "msetnx"
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
        -1
    }
    fn step(&self) -> i64 {
        2
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.pairs.iter().map(|(k, _)| k.clone()).collect()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.pairs
            .iter()
            .flat_map(|(k, v)| [k.clone(), v.clone()])
            .collect()
    }
}
