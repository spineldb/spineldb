// src/core/commands/generic/rename.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::cache_types::{CacheBody, ManifestState};
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;
use tracing::error;

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
        if self.source == self.destination {
            // RENAME to the same key is a no-op that succeeds.
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        let auto_unlink_threshold = ctx
            .state
            .config
            .lock()
            .await
            .safety
            .auto_unlink_on_del_threshold;

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

        // Pop the source value.
        let value_to_move = {
            let source_guard = guards
                .get_mut(&source_shard_index)
                .ok_or_else(|| SpinelDBError::Internal("Missing source lock for RENAME".into()))?;

            // Notify blockers on the source key before it's removed.
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
            source_guard
                .pop(&self.source)
                .filter(|e| !e.is_expired())
                .ok_or(SpinelDBError::KeyNotFound)?
        };

        // Put the value into the destination, overwriting if necessary.
        let old_dest_value = {
            let dest_guard = guards
                .get_mut(&dest_shard_index)
                .ok_or_else(|| SpinelDBError::Internal("Missing dest lock for RENAME".into()))?;

            // Notify blockers on the destination key before it's overwritten.
            if let Some(dest_entry) = dest_guard.peek(&self.destination) {
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
            dest_guard.put(self.destination.clone(), value_to_move.clone())
        };

        // After successfully moving the value, update the on-disk cache manifest if applicable.
        if let DataValue::HttpCache { variants, .. } = &value_to_move.data {
            for variant in variants.values() {
                if let CacheBody::OnDisk { path, .. } = &variant.body {
                    // Log the deletion of the old key's manifest entry and the creation of the new one.
                    ctx.state
                        .cache
                        .log_manifest(
                            self.source.clone(),
                            ManifestState::PendingDelete,
                            path.clone(),
                        )
                        .await?;
                    ctx.state
                        .cache
                        .log_manifest(
                            self.destination.clone(),
                            ManifestState::Committed,
                            path.clone(),
                        )
                        .await?;
                }
            }
        }

        // Handle lazy-free for the old destination value if it was large.
        if let Some(val) = old_dest_value {
            if auto_unlink_threshold > 0 && val.size > auto_unlink_threshold {
                let state_clone = ctx.state.clone();
                let dest_key_clone = self.destination.clone();
                tokio::spawn(async move {
                    let send_timeout = Duration::from_secs(5);
                    if tokio::time::timeout(
                        send_timeout,
                        state_clone
                            .persistence
                            .lazy_free_tx
                            .send(vec![(dest_key_clone, val)]),
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
