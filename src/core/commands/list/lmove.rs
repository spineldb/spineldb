// src/core/commands/list/lmove.rs

//! Implements the `LMOVE` command.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

/// Defines the direction for list operations (left or right).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Side {
    #[default]
    Left,
    Right,
}

/// Represents the `LMOVE` command with its parsed arguments.
#[derive(Debug, Clone, Default)]
pub struct LMove {
    pub source: Bytes,
    pub destination: Bytes,
    pub from: Side,
    pub to: Side,
}

/// A shared, atomic, and safe logic for both `LMOVE` and `BLMOVE`.
pub(crate) async fn lmove_logic<'a>(
    source_key: &Bytes,
    dest_key: &Bytes,
    from: Side,
    to: Side,
    ctx: &mut ExecutionContext<'a>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let guards = match &mut ctx.locks {
        ExecutionLocks::Multi { guards } => guards,
        _ => {
            return Err(SpinelDBError::LockingError(
                "LMOVE requires a multi-key lock".into(),
            ));
        }
    };

    let source_shard_index = ctx.db.get_shard_index(source_key);
    let dest_shard_index = ctx.db.get_shard_index(dest_key);

    // --- Step 1: Peek and take the element from the source (without committing the removal yet) ---
    let popped_value = {
        let source_guard = guards
            .get_mut(&source_shard_index)
            .ok_or_else(|| SpinelDBError::LockingError("Missing source lock for LMOVE".into()))?;

        let Some(source_entry) = source_guard.get_mut(source_key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };
        if source_entry.is_expired() {
            source_guard.pop(source_key); // Passively delete expired key.
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::List(list) = &mut source_entry.data {
            match from {
                Side::Left => list.front().cloned(),
                Side::Right => list.back().cloned(),
            }
        } else {
            return Err(SpinelDBError::WrongType);
        }
    };

    // If there's no element to move, the operation is complete.
    let Some(value_to_move) = popped_value else {
        return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
    };

    // --- Step 2: Push the element to the destination ---
    let dest_shard = ctx.db.get_shard(dest_shard_index);
    {
        let dest_guard = guards.get_mut(&dest_shard_index).ok_or_else(|| {
            SpinelDBError::LockingError("Missing destination lock for LMOVE".into())
        })?;

        // Get or create the destination list.
        let dest_entry = dest_guard.get_or_insert_with_mut(dest_key.clone(), || {
            StoredValue::new(DataValue::List(VecDeque::new()))
        });

        if let DataValue::List(list) = &mut dest_entry.data {
            let val_len = value_to_move.len();
            match to {
                Side::Left => list.push_front(value_to_move.clone()),
                Side::Right => list.push_back(value_to_move.clone()),
            }
            dest_entry.size += val_len;
            dest_entry.version = dest_entry.version.wrapping_add(1);
            dest_shard
                .current_memory
                .fetch_add(val_len, Ordering::Relaxed);
        } else {
            // If the destination key exists but is not a list, return an error.
            return Err(SpinelDBError::WrongType);
        }
    }

    // --- Step 3: Remove the element from the source (after the push succeeded) ---
    let source_shard = ctx.db.get_shard(source_shard_index);
    {
        let source_guard = guards.get_mut(&source_shard_index).ok_or_else(|| {
            SpinelDBError::LockingError("Missing source lock for final pop".into())
        })?;

        let source_entry = source_guard.get_mut(source_key).unwrap(); // Should always exist.

        if let DataValue::List(list) = &mut source_entry.data {
            let popped = match from {
                Side::Left => list.pop_front(),
                Side::Right => list.pop_back(),
            };

            debug_assert_eq!(popped, Some(value_to_move.clone()));

            let val_len = value_to_move.len();
            source_entry.size -= val_len;
            source_entry.version = source_entry.version.wrapping_add(1);
            source_shard
                .current_memory
                .fetch_sub(val_len, Ordering::Relaxed);

            // If the source list becomes empty, remove the key.
            if list.is_empty() {
                source_guard.pop(source_key);
            }
        }
    }

    // --- Step 4: Notify blockers on destination key and finish ---
    // A client might be blocking on the destination key (e.g., via BLPOP).
    // Since we've just added an element, we should wake up any potential waiters.
    // wake_waiters_for_modification is suitable here; it signals that the list has changed,
    // and the woken client will re-attempt its pop operation.
    ctx.state
        .blocker_manager
        .wake_waiters_for_modification(dest_key);

    Ok((
        RespValue::BulkString(value_to_move),
        WriteOutcome::Write { keys_modified: 2 },
    ))
}

impl ParseCommand for LMove {
    /// Parses the `LMOVE` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 4, "LMOVE")?;
        let source = extract_bytes(&args[0])?;
        let destination = extract_bytes(&args[1])?;
        let from_str = extract_string(&args[2])?.to_ascii_lowercase();
        let to_str = extract_string(&args[3])?.to_ascii_lowercase();
        let from = match from_str.as_str() {
            "left" => Side::Left,
            "right" => Side::Right,
            _ => return Err(SpinelDBError::SyntaxError),
        };
        let to = match to_str.as_str() {
            "left" => Side::Left,
            "right" => Side::Right,
            _ => return Err(SpinelDBError::SyntaxError),
        };
        Ok(LMove {
            source,
            destination,
            from,
            to,
        })
    }
}

#[async_trait]
impl ExecutableCommand for LMove {
    /// Executes the `LMOVE` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        lmove_logic(&self.source, &self.destination, self.from, self.to, ctx).await
    }
}

impl CommandSpec for LMove {
    fn name(&self) -> &'static str {
        "lmove"
    }

    fn arity(&self) -> i64 {
        5
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }

    fn first_key(&self) -> i64 {
        1
    }

    fn last_key(&self) -> i64 {
        2
    }

    fn step(&self) -> i64 {
        1
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.source.clone(), self.destination.clone()]
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            self.source.clone(),
            self.destination.clone(),
            (if self.from == Side::Left {
                "LEFT"
            } else {
                "RIGHT"
            })
            .into(),
            (if self.to == Side::Left {
                "LEFT"
            } else {
                "RIGHT"
            })
            .into(),
        ]
    }
}
