// src/core/storage/context.rs

use super::core::Db;
use super::locking::ExecutionLocks;
use super::shard::{DbShard, ShardCache};
use crate::core::Command;
use crate::core::SpinelDBError;
use crate::core::acl::user::AclUser;
use crate::core::commands::command_trait::CommandExt;
use crate::core::state::ServerState;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::MutexGuard;

/// `ExecutionContext` provides all the state and locks required to execute a `Command`.
pub struct ExecutionContext<'a> {
    pub state: Arc<ServerState>,
    pub locks: ExecutionLocks<'a>,
    pub db: &'a Db,
    // Store the command being executed in the context.
    // This makes the context self-contained and simplifies lock helpers.
    pub command: Option<Command>,
    pub session_id: u64,
    /// The ACL user associated with the session, for permission checks.
    pub authenticated_user: Option<Arc<AclUser>>,
}

// --- Implementations for ExecutionContext ---

impl<'a> ExecutionContext<'a> {
    /// A helper function to get the shard and its lock from the context.
    pub fn get_single_shard_context_mut(
        &mut self,
    ) -> Result<(&Arc<DbShard>, &mut MutexGuard<'a, ShardCache>), SpinelDBError> {
        // Get the key directly from the command stored in the context.
        let key = self
            .command
            .as_ref()
            .and_then(|c| c.get_keys().into_iter().next())
            .ok_or_else(|| {
                SpinelDBError::Internal(
                    "Command in context has no keys for single shard lookup.".into(),
                )
            })?;
        let shard_index = self.db.get_shard_index(&key);
        let shard = self.db.get_shard(shard_index);
        match &mut self.locks {
            ExecutionLocks::Single { guard, .. } => Ok((shard, guard)),
            ExecutionLocks::Multi { guards } => {
                let guard = guards.get_mut(&shard_index).ok_or_else(|| {
                    SpinelDBError::LockingError("Required shard lock missing.".into())
                })?;
                Ok((shard, guard))
            }
            _ => Err(SpinelDBError::LockingError(
                "Command expected a single/multi shard lock.".into(),
            )),
        }
    }

    /// Acquires locks for additional keys and merges them with existing locks.
    pub async fn upgrade_locks(&mut self, new_keys: &[Bytes]) {
        let mut current_guards = match std::mem::replace(&mut self.locks, ExecutionLocks::None) {
            ExecutionLocks::Single { shard_index, guard } => {
                let mut map = BTreeMap::new();
                map.insert(shard_index, guard);
                map
            }
            ExecutionLocks::Multi { guards } => guards,
            ExecutionLocks::All { guards } => {
                self.locks = ExecutionLocks::All { guards };
                return;
            }
            ExecutionLocks::None => BTreeMap::new(),
        };
        let mut new_indices = BTreeSet::new();
        for key in new_keys {
            let index = self.db.get_shard_index(key);
            if !current_guards.contains_key(&index) {
                new_indices.insert(index);
            }
        }
        for index in new_indices {
            let guard = self.db.get_shard(index).entries.lock().await;
            current_guards.insert(index, guard);
        }
        self.locks = ExecutionLocks::Multi {
            guards: current_guards,
        };
    }

    /// Releases all locks held by the context.
    pub fn release_locks(&mut self) {
        self.locks = ExecutionLocks::None;
    }

    /// Re-acquires all necessary locks for the command currently in the context.
    pub async fn reacquire_locks_for_command(&mut self) -> Result<(), SpinelDBError> {
        let command = self.command.as_ref().ok_or_else(|| {
            SpinelDBError::Internal("Cannot reacquire locks without a command in context".into())
        })?;
        self.locks = self.db.determine_locks_for_command(command).await;
        Ok(())
    }
}
