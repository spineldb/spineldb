// src/core/database/locking.rs

use super::core::{Db, NUM_SHARDS};
use super::shard::ShardCache;
use crate::core::Command;
use crate::core::commands::command_trait::{CommandExt, CommandFlags};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::MutexGuard;

/// `ExecutionLocks` holds the necessary `MutexGuard`s for executing a command.
/// This enum allows the command execution layer to handle different locking strategies.
pub enum ExecutionLocks<'a> {
    /// A lock on a single database shard.
    Single {
        shard_index: usize,
        guard: MutexGuard<'a, ShardCache>,
    },
    /// Locks on multiple specific database shards.
    Multi {
        guards: BTreeMap<usize, MutexGuard<'a, ShardCache>>,
    },
    /// Locks on all database shards.
    All {
        guards: Vec<MutexGuard<'a, ShardCache>>,
    },
    /// No locks are required for this command.
    None,
}

impl Db {
    /// Determines and acquires the appropriate locks for a given command based on its
    /// type and the keys it operates on.
    pub async fn determine_locks_for_command<'a>(
        &'a self,
        command: &Command,
    ) -> ExecutionLocks<'a> {
        let flags = command.get_flags();
        let keys = command.get_keys();

        // Admin commands that don't operate on keys typically don't need locks.
        if flags.contains(CommandFlags::ADMIN)
            && keys.is_empty()
            && let Command::Acl(_) = command
        {
            return ExecutionLocks::None;
        }

        // Dispatch to the appropriate locking strategy based on the command.
        match command {
            // `KEYS` requires a consistent snapshot of the entire database.
            Command::Keys(_) => ExecutionLocks::All {
                guards: self.lock_all_shards().await,
            },

            // SCAN-family commands manage their own shard-level locking during execution.
            Command::Scan(_) | Command::SScan(_) | Command::HScan(_) | Command::ZScan(_) => {
                ExecutionLocks::None
            }

            // `SORT` needs special handling. A lock on the primary key is acquired initially
            // and can be upgraded later by the command handler.
            Command::Sort(_) => {
                let shard_index = self.get_shard_index(&keys[0]);
                ExecutionLocks::Single {
                    shard_index,
                    guard: self.get_shard(shard_index).entries.lock().await,
                }
            }

            // Commands that handle their own granular locking do not require pre-locking.
            Command::Cache(c)
                if matches!(
                    c.subcommand,
                    crate::core::commands::cache::command::CacheSubcommand::PurgeTag(_)
                ) =>
            {
                ExecutionLocks::None
            }

            // `DbSize` can operate without locks as it uses atomic counters.
            Command::DbSize(_) => ExecutionLocks::None,

            // `FlushDb` operates on the current DB and requires all of its locks.
            Command::FlushDb(_) => ExecutionLocks::All {
                guards: self.lock_all_shards().await,
            },

            // `FlushAll` handles its own cross-DB locking, so the router should not acquire any locks.
            Command::FlushAll(_) => ExecutionLocks::None,

            // Commands operating on multiple keys require locks on all relevant shards.
            _ if keys.len() > 1 => ExecutionLocks::Multi {
                guards: self.lock_shards_for_keys(&keys).await,
            },

            // Commands operating on a single key require a lock on its corresponding shard.
            _ if keys.len() == 1 => {
                let shard_index = self.get_shard_index(&keys[0]);
                ExecutionLocks::Single {
                    shard_index,
                    guard: self.get_shard(shard_index).entries.lock().await,
                }
            }

            // Default for commands with no keys.
            _ => ExecutionLocks::None,
        }
    }

    /// Locks multiple shards based on a list of keys, ensuring a consistent locking order
    /// by sorting shard indices to prevent deadlocks.
    pub async fn lock_shards_for_keys<'a>(
        &'a self,
        keys: &[Bytes],
    ) -> BTreeMap<usize, MutexGuard<'a, ShardCache>> {
        // Collect unique shard indices and sort them to ensure a consistent lock acquisition order.
        let indices: BTreeSet<usize> = keys.iter().map(|key| self.get_shard_index(key)).collect();
        let mut guards = BTreeMap::new();
        for index in indices {
            guards.insert(index, self.shards[index].entries.lock().await);
        }
        guards
    }

    /// Locks all shards in the database, in a fixed order (0 to NUM_SHARDS-1) to prevent deadlocks.
    pub async fn lock_all_shards<'a>(&'a self) -> Vec<MutexGuard<'a, ShardCache>> {
        let mut guards = Vec::with_capacity(NUM_SHARDS);
        for i in 0..NUM_SHARDS {
            guards.push(self.shards[i].entries.lock().await);
        }
        guards
    }
}
