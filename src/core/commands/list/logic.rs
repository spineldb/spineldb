// src/core/commands/list/logic.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::events::{TransactionData, UnitOfWork};
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::{ExecutionContext, PopDirection, PushDirection};
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

    // Atomically hand off the first value to a waiting client if one exists.
    let was_consumed_by_waiter = state
        .blocker_manager
        .notify_and_consume_for_push(key, values[0].clone());

    // If a waiter consumed the value, it bypassed the list entirely.
    // To ensure state consistency, we must manually propagate a transaction
    // that mimics this atomic operation (PUSH followed by a POP) to replicas/AOF.
    if was_consumed_by_waiter {
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

        // The corresponding pop operation to maintain state consistency.
        let pop_cmd = match direction {
            PushDirection::Left => {
                Command::LPop(crate::core::commands::list::LPop { key: key.clone() })
            }
            PushDirection::Right => {
                Command::RPop(crate::core::commands::list::RPop { key: key.clone() })
            }
        };

        // The transaction must contain all commands for AOF, but only writes for replication.
        let tx_data = TransactionData {
            all_commands: vec![push_cmd.clone(), pop_cmd.clone()],
            write_commands: vec![push_cmd, pop_cmd],
        };

        // Manually publish the synthetic transaction.
        ctx.state
            .event_bus
            .publish(UnitOfWork::Transaction(Box::new(tx_data)), &ctx.state);

        // The list length is determined after the remaining items (if any) are pushed.
        let final_len = if let Ok(guard) = ctx
            .db
            .get_shard(ctx.db.get_shard_index(key))
            .entries
            .try_lock()
        {
            guard
                .peek(key)
                .map(|e| match &e.data {
                    DataValue::List(l) => l.len() + values.len() - 1,
                    _ => values.len() - 1,
                })
                .unwrap_or(values.len() - 1)
        } else {
            values.len() - 1 // A reasonable fallback
        };

        // Since the state change was manually propagated, we return DidNotWrite
        // to prevent the command router from propagating it again.
        return Ok((
            RespValue::Integer(final_len as i64),
            WriteOutcome::DidNotWrite,
        ));
    }

    // Standard path: no waiter was available, so we modify the list in storage.
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
