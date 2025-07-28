// src/core/commands/generic/unlink.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::tasks::lazy_free::LazyFreeItem;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;
use tracing::error;

/// Represents the `UNLINK` command.
#[derive(Debug, Clone, Default)]
pub struct Unlink {
    pub keys: Vec<Bytes>,
}

impl ParseCommand for Unlink {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("UNLINK".to_string()));
        }
        let keys = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Unlink { keys })
    }
}

#[async_trait]
impl ExecutableCommand for Unlink {
    /// Executes the UNLINK command by removing keys from the keyspace and sending them
    /// to the lazy-free manager for background deallocation.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut count = 0u64;
        let mut items_to_reclaim: Vec<LazyFreeItem> = Vec::new();
        let mut post_lock_tasks: Vec<(Bytes, DataValue)> = Vec::new();

        // --- Start of Locking Scope ---
        {
            let guards = match &mut ctx.locks {
                ExecutionLocks::Multi { guards } => guards,
                _ => {
                    return Err(SpinelDBError::Internal(
                        "UNLINK requires multi-key lock but was not provided.".into(),
                    ));
                }
            };

            for key in &self.keys {
                let shard_index = ctx.db.get_shard_index(key);
                if let Some(guard) = guards.get_mut(&shard_index) {
                    // `pop` removes the key and returns its value if it existed.
                    if let Some(popped_value) = guard.pop(key) {
                        if !popped_value.is_expired() {
                            count += 1;
                            // Defer notification and reclamation to avoid holding locks across await points.
                            post_lock_tasks.push((key.clone(), popped_value.data.clone()));
                            items_to_reclaim.push((key.clone(), popped_value));
                        }
                    }
                }
            }
        } // --- End of Locking Scope ---

        // Execute post-lock notification tasks now that locks are released.
        for (key, data_value) in post_lock_tasks {
            match data_value {
                DataValue::Stream(_) => {
                    ctx.state.stream_blocker_manager.notify_and_remove_all(&key);
                }
                DataValue::List(_) | DataValue::SortedSet(_) => {
                    ctx.state
                        .blocker_manager
                        .wake_waiters_for_modification(&key);
                }
                _ => {}
            }
        }

        // Send the (key, value) pairs to the lazy-free manager for background reclamation.
        if !items_to_reclaim.is_empty() {
            let state_clone = ctx.state.clone();
            // Spawn a new task to send to the lazy-free channel.
            // This prevents the command from blocking if the channel is full.
            tokio::spawn(async move {
                let send_timeout = Duration::from_secs(5);

                // Use a timeout to prevent indefinite blocking if the lazy-free task is stuck.
                if tokio::time::timeout(
                    send_timeout,
                    state_clone.persistence.lazy_free_tx.send(items_to_reclaim),
                )
                .await
                .is_err()
                {
                    error!(
                        "Failed to send to lazy-free channel within 5 seconds. The task may be unresponsive or have panicked."
                    );
                    state_clone.persistence.increment_lazy_free_errors();
                    // As a safety measure, put the server into read-only mode.
                    state_clone.set_read_only(true, "Lazy-free task is unresponsive.");
                }
            });
        }

        let outcome = if count > 0 {
            WriteOutcome::Delete {
                keys_deleted: count,
            }
        } else {
            WriteOutcome::DidNotWrite
        };

        Ok((RespValue::Integer(count as i64), outcome))
    }
}

impl CommandSpec for Unlink {
    fn name(&self) -> &'static str {
        "unlink"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
}
