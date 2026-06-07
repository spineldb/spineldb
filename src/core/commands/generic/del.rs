// src/core/commands/generic/del.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::tasks::lazy_free::LazyFreeItem;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

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
                if let Some(guard) = guards.get_mut(&shard_index)
                    && let Some(popped_value) = guard.pop(key)
                    && !popped_value.is_expired()
                {
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
            // Use try_send for a non-blocking attempt to offload work.
            match ctx.state.persistence.lazy_free_tx.try_send(items_to_unlink) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_del_with_single_key() {
        let d = Del::parse(&[bs("k1")]).unwrap();
        assert_eq!(d.keys, vec![Bytes::from_static(b"k1")]);
    }

    #[test]
    fn test_del_with_many_keys() {
        let d = Del::parse(&[bs("a"), bs("b"), bs("c")]).unwrap();
        assert_eq!(d.keys.len(), 3);
        assert_eq!(d.keys[0], Bytes::from_static(b"a"));
        assert_eq!(d.keys[2], Bytes::from_static(b"c"));
    }

    #[test]
    fn test_del_with_no_args_is_error() {
        let r = Del::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_del_with_non_bulk_is_wrong_type() {
        let r = Del::parse(&[bs("k"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_del_default_is_empty() {
        let d = Del::default();
        assert!(d.keys.is_empty());
    }

    #[test]
    fn test_del_to_resp_args_round_trips() {
        let d = Del::parse(&[bs("x"), bs("y")]).unwrap();
        let args = d.to_resp_args();
        assert_eq!(args, d.keys);
    }
}
