// src/core/commands/zset/zpop_logic.rs

use crate::core::commands::command_trait::ExecutableCommand;
use crate::core::database::ExecutionContext;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError, commands::command_trait::WriteOutcome};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

/// Defines the side from which to pop elements in a sorted set.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum PopSide {
    #[default]
    Min,
    Max,
}

/// The internal struct for ZPOP logic, shared by ZPOPMIN and ZPOPMAX.
#[derive(Debug, Clone, Default)]
pub struct ZPop {
    pub key: Bytes,
    pub(super) side: PopSide,
    pub count: Option<usize>,
}

impl ZPop {
    pub(super) fn new(key: Bytes, side: PopSide, count: Option<usize>) -> Self {
        Self { key, side, count }
    }
}

#[async_trait]
impl ExecutableCommand for ZPop {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Delegate to the shared logic function.
        zpop_logic(ctx, &self.key, self.side, self.count).await
    }
}

/// The internal shared logic for `ZPOPMIN` and `ZPOPMAX`.
pub(crate) async fn zpop_logic<'a>(
    ctx: &mut ExecutionContext<'a>,
    key: &Bytes,
    side: PopSide,
    count: Option<usize>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let empty_response = if count.is_none() {
        RespValue::Null
    } else {
        RespValue::Array(vec![])
    };

    let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
    let Some(entry) = shard_cache_guard.get_mut(key) else {
        return Ok((empty_response, WriteOutcome::DidNotWrite));
    };
    if entry.is_expired() {
        shard_cache_guard.pop(key);
        return Ok((empty_response, WriteOutcome::DidNotWrite));
    }

    if let DataValue::SortedSet(zset) = &mut entry.data {
        if zset.is_empty() {
            return Ok((empty_response, WriteOutcome::DidNotWrite));
        }

        let count_to_pop = count.unwrap_or(1);
        if count_to_pop == 0 {
            return Ok((empty_response, WriteOutcome::DidNotWrite));
        }

        let mut popped_entries = Vec::with_capacity(count_to_pop);

        for _ in 0..count_to_pop {
            let popped = match side {
                PopSide::Min => zset.pop_first(),
                PopSide::Max => zset.pop_last(),
            };
            if let Some(p) = popped {
                popped_entries.push(p);
            } else {
                break;
            }
        }

        if popped_entries.is_empty() {
            return Ok((empty_response, WriteOutcome::DidNotWrite));
        }

        let old_mem = entry.size;
        let new_mem = zset.memory_usage();
        entry.size = new_mem;
        entry.version = entry.version.wrapping_add(1);
        if new_mem < old_mem {
            shard
                .current_memory
                .fetch_sub(old_mem - new_mem, Ordering::Relaxed);
        }

        let outcome = if zset.is_empty() {
            shard_cache_guard.pop(key);
            WriteOutcome::Delete { keys_deleted: 1 }
        } else {
            WriteOutcome::Write { keys_modified: 1 }
        };

        let mut resp_vec = Vec::with_capacity(popped_entries.len() * 2);
        for p in popped_entries {
            resp_vec.push(RespValue::BulkString(p.member));
            resp_vec.push(RespValue::BulkString(p.score.to_string().into()));
        }

        Ok((RespValue::Array(resp_vec), outcome))
    } else {
        Err(SpinelDBError::WrongType)
    }
}
