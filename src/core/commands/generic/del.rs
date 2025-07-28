// src/core/commands/generic/del.rs

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
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::error;

/// Represents the `DEL` command.
#[derive(Debug, Clone, Default)]
pub struct Del {
    pub keys: Vec<Bytes>,
}

impl ParseCommand for Del {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("DEL".to_string()));
        }
        let keys = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Del { keys })
    }
}

#[async_trait]
impl ExecutableCommand for Del {
    /// Executes the DEL command, with logic to auto-UNLINK large values.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut count = 0u64;

        let auto_unlink_threshold = ctx
            .state
            .config
            .lock()
            .await
            .safety
            .auto_unlink_on_del_threshold;

        // Collect tasks to be performed after releasing the database locks.
        let mut post_lock_tasks: Vec<(Bytes, DataValue)> = Vec::new();
        let mut items_to_unlink: Vec<LazyFreeItem> = Vec::new();

        // --- Start of Locking Scope ---
        {
            let mut guards = match std::mem::replace(&mut ctx.locks, ExecutionLocks::None) {
                ExecutionLocks::Multi { guards } => guards,
                ExecutionLocks::Single { shard_index, guard } => {
                    let mut map = BTreeMap::new();
                    map.insert(shard_index, guard);
                    map
                }
                _ => {
                    return Err(SpinelDBError::Internal(
                        "DEL requires appropriate lock (Single or Multi)".into(),
                    ));
                }
            };

            for key in &self.keys {
                let shard_index = ctx.db.get_shard_index(key);
                if let Some(guard) = guards.get_mut(&shard_index) {
                    if let Some(popped_value) = guard.pop(key) {
                        if !popped_value.is_expired() {
                            count += 1;

                            // Defer notification to avoid holding a shard lock while
                            // acquiring a blocker lock.
                            post_lock_tasks.push((key.clone(), popped_value.data.clone()));

                            // Check if this value should be auto-unlinked.
                            let should_unlink = (auto_unlink_threshold > 0
                                && popped_value.size > auto_unlink_threshold)
                                || matches!(popped_value.data, DataValue::HttpCache { .. });

                            if should_unlink {
                                // Send both key and value to the lazy-free manager.
                                items_to_unlink.push((key.clone(), popped_value));
                            }
                        }
                    }
                }
            }
        } // --- End of Locking Scope: All shard locks are released here. ---

        // Execute post-lock tasks now that locks are released.
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

        // Dispatch values to the lazy-free thread if necessary.
        if !items_to_unlink.is_empty() {
            let state_clone = ctx.state.clone();
            // Spawn a new task to send to the lazy-free channel.
            tokio::spawn(async move {
                let send_timeout = Duration::from_secs(5);

                // Use a timeout to prevent indefinite blocking if the lazy-free task is stuck.
                if tokio::time::timeout(
                    send_timeout,
                    state_clone.persistence.lazy_free_tx.send(items_to_unlink),
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

impl CommandSpec for Del {
    fn name(&self) -> &'static str {
        "del"
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
