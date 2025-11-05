// src/core/commands/set/smismember.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct SMIsMember {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ParseCommand for SMIsMember {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, members) = parse_key_and_values(args, 2, "SMISMEMBER")?;
        Ok(SMIsMember { key, members })
    }
}

#[async_trait]
impl ExecutableCommand for SMIsMember {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let set_ref: Option<&HashSet<Bytes>> =
            if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
                if entry.is_expired() {
                    None
                } else if let DataValue::Set(set) = &entry.data {
                    Some(set)
                } else {
                    return Err(SpinelDBError::WrongType);
                }
            } else {
                None
            };

        let results: Vec<RespValue> = self
            .members
            .iter()
            .map(|member| {
                let is_member = set_ref.is_some_and(|s| s.contains(member));
                RespValue::Integer(is_member as i64)
            })
            .collect();

        Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for SMIsMember {
    fn name(&self) -> &'static str {
        "smismember"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone()];
        args.extend_from_slice(&self.members);
        args
    }
}
