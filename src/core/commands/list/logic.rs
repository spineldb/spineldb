// src/core/commands/list/logic.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::{ExecutionContext, PopDirection, PushDirection};
use crate::core::events::{TransactionData, UnitOfWork};
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{Command, RespValue, SpinelDBError};
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
    // If no values are provided, the command returns the current length of the list.
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

    // Attempt to atomically hand off the first value to a waiting client (from BLPOP etc.).
    // This bypasses the list storage entirely for that value if successful.
    if let Some(final_len) = state
        .blocker_manager
        .notify_and_consume_for_push(key, values)
    {
        // A waiter consumed the value, so it bypassed the list. To ensure state consistency,
        // manually propagate a transaction that mimics this atomic operation (PUSH then POP).
        let push_cmd = match direction {
            PushDirection::Left => Command::LPush(crate::core::commands::list::LPush {
                key: key.clone(),
                values: values.to_vec(),
            }),
            PushDirection::Right => Command::RPush(crate::core::commands::list::RPush {
                key: key.clone(),
                values: values.to_vec(),
            }),
        };

        // The corresponding pop operation to maintain state consistency in AOF/replication.
        let pop_cmd = match direction {
            PushDirection::Left => {
                Command::LPop(crate::core::commands::list::LPop { key: key.clone() })
            }
            PushDirection::Right => {
                Command::RPop(crate::core::commands::list::RPop { key: key.clone() })
            }
        };

        let tx_data = TransactionData {
            all_commands: vec![push_cmd.clone(), pop_cmd.clone()],
            write_commands: vec![push_cmd, pop_cmd],
        };

        // Manually publish the synthetic transaction to the event bus.
        ctx.state
            .event_bus
            .publish(UnitOfWork::Transaction(Box::new(tx_data)), &ctx.state);

        // The length of the list is returned directly from the notifier, ensuring an
        // accurate value without race conditions.
        return Ok((
            RespValue::Integer(final_len as i64),
            WriteOutcome::DidNotWrite, // Propagation is handled manually.
        ));
    }

    // Standard path: no waiter was available, so modify the list in storage.
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
        shard.update_memory(total_added_size as isize);

        let final_len = list.len() as i64;
        let outcome = WriteOutcome::Write { keys_modified: 1 };

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
