// src/core/commands/zset/zincrby.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::commands::zset::zpop_logic::PopSide;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::db::zset::SortedSet;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZIncrBy {
    pub key: Bytes,
    pub increment: f64,
    pub member: Bytes,
}
impl ParseCommand for ZIncrBy {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "ZINCRBY")?;
        Ok(ZIncrBy {
            key: extract_bytes(&args[0])?,
            increment: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAFloat)?,
            member: extract_bytes(&args[2])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ZIncrBy {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state_clone = ctx.state.clone();
        let (shard, guard) = ctx.get_single_shard_context_mut()?;

        let entry = guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::SortedSet(SortedSet::new()))
        });

        if let DataValue::SortedSet(zset) = &mut entry.data {
            let old_mem = zset.memory_usage();
            let new_score = zset.increment_score(&self.member, self.increment);

            // Notify after incrementing, as the order might have changed.
            state_clone
                .blocker_manager
                .notify_and_pop_zset_waiter(zset, &self.key, PopSide::Min);

            // Recalculate memory and update metadata based on the final state.
            let new_mem = zset.memory_usage();
            entry.version = entry.version.wrapping_add(1);
            entry.size = new_mem;
            if new_mem > old_mem {
                shard.update_memory((new_mem - old_mem) as isize);
            } else {
                shard.update_memory(-((old_mem - new_mem) as isize));
            }
            Ok((
                RespValue::BulkString(new_score.to_string().into()),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}
impl CommandSpec for ZIncrBy {
    fn name(&self) -> &'static str {
        "zincrby"
    }
    fn arity(&self) -> i64 {
        4
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
        vec![
            self.key.clone(),
            self.increment.to_string().into(),
            self.member.clone(),
        ]
    }
}
