// src/core/commands/generic/exists.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct Exists {
    pub keys: Vec<Bytes>,
}
impl ParseCommand for Exists {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("EXISTS".to_string()));
        }
        let keys = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Exists { keys })
    }
}
#[async_trait]
impl ExecutableCommand for Exists {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut count = 0;
        let mut guards = match std::mem::replace(&mut ctx.locks, ExecutionLocks::None) {
            ExecutionLocks::Multi { guards } => guards,
            ExecutionLocks::Single { shard_index, guard } => {
                let mut map = BTreeMap::new();
                map.insert(shard_index, guard);
                map
            }
            _ => {
                return Err(SpinelDBError::Internal(
                    "EXISTS requires appropriate lock (Single or Multi)".into(),
                ));
            }
        };

        for key in &self.keys {
            let shard_index = ctx.db.get_shard_index(key);
            if let Some(guard) = guards.get_mut(&shard_index) {
                if let Some(entry) = guard.peek(key) {
                    if !entry.is_expired() {
                        count += 1;
                    }
                }
            }
        }

        Ok((RespValue::Integer(count), WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for Exists {
    fn name(&self) -> &'static str {
        "exists"
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
