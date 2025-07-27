// src/core/commands/list/logic.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::{ExecutionContext, PopDirection, PushDirection};
use crate::core::{RespValue, SpinelDBError};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

/// Shared logic for `LPUSH` and `RPUSH` commands.
pub(crate) async fn list_push_logic<'a>(
    ctx: &mut ExecutionContext<'a>,
    key: &Bytes,
    values: &[Bytes],
    direction: PushDirection,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    // If no values are provided, return the current length of the list, or 0 if it doesn't exist.
    if values.is_empty() {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let len = if let Some(entry) = shard_cache_guard.get(key) {
            if entry.is_expired() {
                0
            } else if let DataValue::List(l) = &entry.data {
                l.len()
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            0
        };
        return Ok((RespValue::Integer(len as i64), WriteOutcome::DidNotWrite));
    }

    let state = ctx.state.clone();

    // **ATOMICITY FIX**: First, try to hand off the first value to a waiting client.
    // If successful, that value is considered "consumed" and is NOT added to the list.
    // The rest of the values are then added. This is the Redis-compatible, race-free pattern.
    let was_consumed_by_waiter = state
        .blocker_manager
        .notify_and_consume_for_push(key, values[0].clone());

    let values_to_actually_push = if was_consumed_by_waiter {
        &values[1..]
    } else {
        values
    };

    let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
    let entry = shard_cache_guard.get_or_insert_with_mut(key.clone(), || {
        StoredValue::new(DataValue::List(VecDeque::new()))
    });

    if let DataValue::List(list) = &mut entry.data {
        if !values_to_actually_push.is_empty() {
            let mut total_added_size = 0;
            // Note: For LPUSH, Redis pushes elements one by one from left to right,
            // so the arguments appear reversed in the list. Our iterator does this naturally.
            for value in values_to_actually_push {
                total_added_size += value.len();
                match direction {
                    PushDirection::Left => list.push_front(value.clone()),
                    PushDirection::Right => list.push_back(value.clone()),
                }
            }
            entry.version = entry.version.wrapping_add(1);
            entry.size += total_added_size;
            shard.update_memory(total_added_size as isize);
        }

        let final_len = list.len() as i64;

        // The write outcome is determined by whether we actually modified the list in storage.
        let outcome = if !values_to_actually_push.is_empty() {
            WriteOutcome::Write { keys_modified: 1 }
        } else {
            // If the value was consumed by a waiter, the state was changed conceptually,
            // but the list key itself was not modified on this node. Redis does not
            // replicate the PUSH in this case, so DidNotWrite is correct.
            WriteOutcome::DidNotWrite
        };

        Ok((RespValue::Integer(final_len), outcome))
    } else {
        Err(SpinelDBError::WrongType)
    }
}

/// Shared logic for `LPOP` and `RPOP` commands.
pub(crate) async fn list_pop_logic<'a>(
    ctx: &mut ExecutionContext<'a>,
    key: &Bytes,
    direction: PopDirection,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

    let Some(entry) = shard_cache_guard.get_mut(key) else {
        return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
    };
    if entry.is_expired() {
        shard_cache_guard.pop(key);
        return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
    }

    if let DataValue::List(list) = &mut entry.data {
        let popped_value = match direction {
            PopDirection::Left => list.pop_front(),
            PopDirection::Right => list.pop_back(),
        };

        if let Some(val) = popped_value {
            entry.version = entry.version.wrapping_add(1);
            let val_len = val.len();
            entry.size -= val_len;
            shard.current_memory.fetch_sub(val_len, Ordering::Relaxed);

            let is_now_empty = list.is_empty();

            let outcome = if is_now_empty {
                shard_cache_guard.pop(key);
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::Write { keys_modified: 1 }
            };

            Ok((RespValue::BulkString(val), outcome))
        } else {
            Ok((RespValue::Null, WriteOutcome::DidNotWrite))
        }
    } else {
        Err(SpinelDBError::WrongType)
    }
}
