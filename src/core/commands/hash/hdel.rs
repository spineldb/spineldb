// src/core/commands/hash/hdel.rs

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

#[derive(Debug, Clone, Default)]
pub struct HDel {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}
impl ParseCommand for HDel {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, fields) = parse_key_and_values(args, 2, "HDEL")?;
        Ok(HDel { key, fields })
    }
}
#[async_trait]
impl ExecutableCommand for HDel {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            // Key does not exist, nothing to delete.
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            // Expired key is treated as non-existent for this operation.
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        // Scope the mutable borrow of `entry.data` to modify the hash first.
        let (deleted_count, is_now_empty) = {
            if let DataValue::Hash(hash) = &mut entry.data {
                let mut count = 0;
                for field in &self.fields {
                    if hash.swap_remove(field).is_some() {
                        count += 1;
                    }
                }
                (count, hash.is_empty())
            } else {
                return Err(SpinelDBError::WrongType);
            }
        };

        // If no fields were actually deleted, it's a no-op.
        if deleted_count == 0 {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let outcome = if is_now_empty {
            // The hash became empty, so remove the entire key.
            // `ShardCache::pop` handles all memory accounting.
            shard_cache_guard.pop(&self.key);
            WriteOutcome::Delete { keys_deleted: 1 }
        } else {
            // The hash was modified but is not empty.
            // Recalculate size and update version.
            let entry = shard_cache_guard.get_mut(&self.key).unwrap(); // Should always exist here
            entry.size = entry.data.memory_usage();
            entry.version = entry.version.wrapping_add(1);
            WriteOutcome::Write { keys_modified: 1 }
        };

        Ok((RespValue::Integer(deleted_count as i64), outcome))
    }
}
impl CommandSpec for HDel {
    fn name(&self) -> &'static str {
        "hdel"
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
        args.extend(self.fields.clone());
        args
    }
}
