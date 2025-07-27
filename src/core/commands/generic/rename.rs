// src/core/commands/generic/rename.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;
use tracing::{error, warn};

#[derive(Debug, Clone, Default)]
pub struct Rename {
    pub source: Bytes,
    pub destination: Bytes,
}

impl ParseCommand for Rename {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "RENAME")?;
        Ok(Rename {
            source: extract_bytes(&args[0])?,
            destination: extract_bytes(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Rename {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Handle the trivial case where source and destination are the same.
        if self.source == self.destination {
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        // Get the lazy-free threshold for potentially large overwritten values.
        let auto_unlink_threshold = ctx
            .state
            .config
            .lock()
            .await
            .safety
            .auto_unlink_on_del_threshold;

        // Acquire locks for both source and destination keys.
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "RENAME requires multi-key lock".into(),
                ));
            }
        };

        let source_shard_index = ctx.db.get_shard_index(&self.source);
        let dest_shard_index = ctx.db.get_shard_index(&self.destination);

        // Step 1: Peek at the source value without removing it. If it doesn't exist, return an error.
        let value_to_move = {
            let source_guard = guards
                .get(&source_shard_index)
                .ok_or_else(|| SpinelDBError::Internal("Missing source lock for RENAME".into()))?;

            source_guard
                .peek(&self.source)
                .filter(|e| !e.is_expired())
                .cloned()
                .ok_or(SpinelDBError::KeyNotFound)?
        };

        // Step 2: Place the value at the destination, overwriting any existing value.
        let old_dest_value = {
            let dest_guard = guards
                .get_mut(&dest_shard_index)
                .ok_or_else(|| SpinelDBError::Internal("Missing dest lock for RENAME".into()))?;

            // Notify any clients blocked on the destination key before it is overwritten.
            if let Some(dest_entry) = dest_guard.peek(&self.destination) {
                if std::mem::discriminant(&dest_entry.data)
                    != std::mem::discriminant(&value_to_move.data)
                {
                    warn!(
                        "RENAME is overwriting key '{}' which has a different type than source key '{}'.",
                        String::from_utf8_lossy(&self.destination),
                        String::from_utf8_lossy(&self.source)
                    );
                }
                match &dest_entry.data {
                    DataValue::List(_) | DataValue::SortedSet(_) => {
                        ctx.state
                            .blocker_manager
                            .wake_waiters_for_modification(&self.destination);
                    }
                    DataValue::Stream(_) => {
                        ctx.state
                            .stream_blocker_manager
                            .notify_and_remove_all(&self.destination);
                    }
                    _ => {}
                }
            }
            // `put` overwrites and returns the previous value if it existed.
            dest_guard.put(self.destination.clone(), value_to_move)
        };

        // Step 3: Now that the value is safely at the destination, remove the source key.
        {
            let source_guard = guards.get_mut(&source_shard_index).ok_or_else(|| {
                SpinelDBError::Internal("Missing source lock for RENAME pop".into())
            })?;

            // Notify any clients blocked on the source key before it is removed.
            if let Some(entry) = source_guard.peek(&self.source) {
                match &entry.data {
                    DataValue::List(_) | DataValue::SortedSet(_) => {
                        ctx.state
                            .blocker_manager
                            .wake_waiters_for_modification(&self.source);
                    }
                    DataValue::Stream(_) => {
                        ctx.state
                            .stream_blocker_manager
                            .notify_and_remove_all(&self.source);
                    }
                    _ => {}
                }
            }
            // This operation should always succeed as we confirmed existence in Step 1.
            source_guard.pop(&self.source);
        }

        // Step 4: If the destination key was overwritten, dispatch the old value for lazy-freeing if it's large.
        if let Some(val) = old_dest_value {
            if auto_unlink_threshold > 0 && val.size > auto_unlink_threshold {
                let state_clone = ctx.state.clone();
                tokio::spawn(async move {
                    let send_timeout = Duration::from_secs(5);
                    if tokio::time::timeout(
                        send_timeout,
                        state_clone.persistence.lazy_free_tx.send(vec![val]),
                    )
                    .await
                    .is_err()
                    {
                        error!(
                            "Failed to send to lazy-free channel within 5 seconds during RENAME. The task may be unresponsive or have panicked."
                        );
                        state_clone.persistence.increment_lazy_free_errors();
                        state_clone.set_read_only(true, "Lazy-free task is unresponsive.");
                    }
                });
            }
        }

        // Step 5: Return success.
        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 2 },
        ))
    }
}

impl CommandSpec for Rename {
    fn name(&self) -> &'static str {
        "rename"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        vec![self.source.clone(), self.destination.clone()]
    }
}
