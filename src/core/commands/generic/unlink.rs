// src/core/commands/generic/unlink.rs

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
use std::time::Duration;
use tracing::error;

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
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut count = 0u64;
        let mut values_to_reclaim: Vec<StoredValue> = Vec::new();

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
                        values_to_reclaim.push(popped_value);
                    }
                }
            }
        }

        if !values_to_reclaim.is_empty() {
            let state_clone = ctx.state.clone();
            tokio::spawn(async move {
                let send_timeout = Duration::from_secs(5);
                if tokio::time::timeout(
                    send_timeout,
                    state_clone.persistence.lazy_free_tx.send(values_to_reclaim),
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
