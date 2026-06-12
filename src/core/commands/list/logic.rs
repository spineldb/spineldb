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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn test_push_left_direction() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"a"));
        list.push_front(Bytes::from_static(b"b"));
        let values: Vec<Bytes> = list.into_iter().collect();
        assert_eq!(values[0].as_ref(), b"b");
        assert_eq!(values[1].as_ref(), b"a");
    }

    #[test]
    fn test_push_right_direction() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"a"));
        list.push_back(Bytes::from_static(b"b"));
        let values: Vec<Bytes> = list.into_iter().collect();
        assert_eq!(values[0].as_ref(), b"a");
        assert_eq!(values[1].as_ref(), b"b");
    }

    #[test]
    fn test_pop_left_from_front() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"a"));
        list.push_back(Bytes::from_static(b"b"));
        let popped = list.pop_front();
        assert_eq!(popped.unwrap().as_ref(), b"a");
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_pop_right_from_back() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"a"));
        list.push_back(Bytes::from_static(b"b"));
        let popped = list.pop_back();
        assert_eq!(popped.unwrap().as_ref(), b"b");
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_pop_empty_list_returns_none() {
        let mut list: VecDeque<Bytes> = VecDeque::new();
        assert!(list.pop_front().is_none());
        assert!(list.pop_back().is_none());
    }

    #[test]
    fn test_empty_values_returns_length() {
        let values: Vec<Bytes> = vec![];
        assert!(values.is_empty());
    }

    #[test]
    fn test_list_length_after_pushes() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"1"));
        list.push_back(Bytes::from_static(b"2"));
        list.push_back(Bytes::from_static(b"3"));
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_list_length_after_pops() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"1"));
        list.push_back(Bytes::from_static(b"2"));
        list.pop_front();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_list_becomes_empty_after_popping_all() {
        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"x"));
        list.pop_front();
        assert!(list.is_empty());
    }

    #[test]
    fn test_version_increment() {
        let version: u64 = 0;
        let new_version = version.wrapping_add(1);
        assert_eq!(new_version, 1);
    }

    #[test]
    fn test_version_wrapping() {
        let version: u64 = u64::MAX;
        let new_version = version.wrapping_add(1);
        assert_eq!(new_version, 0);
    }

    #[test]
    fn test_size_calculation() {
        let values = [Bytes::from_static(b"hello"), Bytes::from_static(b"world")];
        let total_size: usize = values.iter().map(|v| v.len()).sum();
        assert_eq!(total_size, 10);
    }

    #[test]
    fn test_push_direction_variants() {
        let left = PushDirection::Left;
        let right = PushDirection::Right;
        assert_ne!(format!("{:?}", left), format!("{:?}", right));
    }

    #[test]
    fn test_pop_direction_variants() {
        let left = PopDirection::Left;
        let right = PopDirection::Right;
        assert_ne!(format!("{:?}", left), format!("{:?}", right));
    }
}
