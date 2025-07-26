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
    let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
    let entry = shard_cache_guard.get_or_insert_with_mut(key.clone(), || {
        StoredValue::new(DataValue::List(VecDeque::new()))
    });

    if let DataValue::List(list) = &mut entry.data {
        let mut total_added_size = 0;
        for value in values {
            total_added_size += value.len();
            match direction {
                PushDirection::Left => list.push_front(value.clone()),
                PushDirection::Right => list.push_back(value.clone()),
            }
        }
        entry.version = entry.version.wrapping_add(1);
        entry.size += total_added_size;
        shard
            .current_memory
            .fetch_add(total_added_size, Ordering::Relaxed);

        let final_len = list.len() as i64;

        // Try to notify a waiting client (BLPOP/BLMOVE). The notifier will atomically
        // pop the element and send it to the waiter if one exists.
        let was_popped_for_waiter = state.blocker_manager.notify_waiters(
            key,
            values[0].clone(), // Send the first pushed element
        );

        if was_popped_for_waiter {
            // If the element was consumed by a waiter, remove it from the list
            // to reflect its immediate consumption.
            let popped_value = match direction {
                PushDirection::Left => list.pop_front(),
                PushDirection::Right => list.pop_back(),
            };

            // Sanity check: the popped value should be the same as the one sent to the notifier.
            debug_assert_eq!(popped_value.as_ref(), Some(&values[0]));

            if let Some(val) = popped_value {
                let val_len = val.len();
                entry.size -= val_len;
                shard.current_memory.fetch_sub(val_len, Ordering::Relaxed);
            }
        }

        Ok((
            RespValue::Integer(final_len),
            WriteOutcome::Write { keys_modified: 1 },
        ))
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
