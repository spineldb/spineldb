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
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

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
            // Use try_send for a non-blocking attempt to offload work.
            match ctx
                .state
                .persistence
                .lazy_free_tx
                .try_send(items_to_reclaim)
            {
                Ok(_) => {}
                Err(TrySendError::Full(items)) => {
                    // This is a critical state where the background task cannot keep up.
                    // Instead of blocking or spawning, we log, increment a metric,
                    // and let the items be dropped synchronously. This provides backpressure.
                    warn!(
                        "Lazy-free channel is full. Deallocating {} items synchronously.",
                        items.len()
                    );
                    ctx.state.persistence.increment_lazy_free_errors();
                }
                Err(TrySendError::Closed(_)) => {
                    // The lazy-free task has terminated, which is a critical failure.
                    let reason = "Lazy-free task is not running.".to_string();
                    warn!("CRITICAL: {reason}");
                    ctx.state.set_read_only(true, &reason);
                }
            }
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
