// src/core/commands/generic/del.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::error;

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

        let mut values_to_unlink: Vec<StoredValue> = Vec::new();

        for key in &self.keys {
            let shard_index = ctx.db.get_shard_index(key);
            if let Some(guard) = guards.get_mut(&shard_index) {
                if let Some(entry) = guard.peek(key) {
                    match &entry.data {
                        DataValue::Stream(_) => {
                            ctx.state.stream_blocker_manager.notify_and_remove_all(key);
                        }
                        DataValue::List(_) | DataValue::SortedSet(_) => {
                            ctx.state.blocker_manager.notify_waiters(key, Bytes::new());
                        }
                        _ => {}
                    }
                }

                if let Some(popped_value) = guard.pop(key) {
                    if !popped_value.is_expired() {
                        count += 1;

                        let should_unlink = (auto_unlink_threshold > 0
                            && popped_value.size > auto_unlink_threshold)
                            || matches!(popped_value.data, DataValue::HttpCache { .. });

                        if should_unlink {
                            values_to_unlink.push(popped_value);
                        }
                    }
                }
            }
        }

        if !values_to_unlink.is_empty() {
            let state_clone = ctx.state.clone();
            tokio::spawn(async move {
                let send_timeout = Duration::from_secs(5);
                if tokio::time::timeout(
                    send_timeout,
                    state_clone.persistence.lazy_free_tx.send(values_to_unlink),
                )
                .await
                .is_err()
                {
                    error!(
                        "Failed to send to lazy-free channel within 5 seconds. The task may be unresponsive or have panicked."
                    );
                    state_clone.persistence.increment_lazy_free_errors();
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
