// src/core/commands/set/srem.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct Srem {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ParseCommand for Srem {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, members) = parse_key_and_values(args, 2, "SREM")?;
        Ok(Srem { key, members })
    }
}

#[async_trait]
impl ExecutableCommand for Srem {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        if let DataValue::Set(set) = &mut entry.data {
            let mut removed_count = 0;
            let mut mem_freed = 0;

            for member in &self.members {
                if set.remove(member) {
                    removed_count += 1;
                    mem_freed += member.len();
                }
            }

            if removed_count == 0 {
                return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
            }

            // Update metadata jika ada perubahan
            let is_now_empty = set.is_empty();
            entry.version = entry.version.wrapping_add(1);
            entry.size -= mem_freed;
            shard.current_memory.fetch_sub(mem_freed, Ordering::Relaxed);

            let outcome = if is_now_empty {
                shard_cache_guard.pop(&self.key);
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::Write { keys_modified: 1 }
            };

            Ok((RespValue::Integer(removed_count as i64), outcome))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

// Implementasi CommandSpec tidak perlu diubah.
impl CommandSpec for Srem {
    fn name(&self) -> &'static str {
        "srem"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        args.extend(self.members.clone());
        args
    }
}
