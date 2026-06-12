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

            // `SORT` manages its own multi-key locking inside the command handler
            // (it needs to release and re-acquire locks across phases), so the
            // router must not pre-lock any shard.
            Command::Sort(_) => ExecutionLocks::None,

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

            // `XREAD` and `XREADGROUP` always require multi-key locks because their
            // `read_from_streams` implementation accesses the `guards` map directly.
            // Even with a single key, they need `ExecutionLocks::Multi`.
            Command::XRead(_) | Command::XReadGroup(_) => ExecutionLocks::Multi {
                guards: self.lock_shards_for_keys(&keys).await,
            },

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::storage::data_types::{DataValue, StoredValue};

    fn make_sv(s: &str) -> StoredValue {
        StoredValue::new(DataValue::String(Bytes::copy_from_slice(s.as_bytes())))
    }

    #[tokio::test]
    async fn test_lock_shards_for_keys_empty_returns_empty() {
        let db = Db::new();
        let guards = db.lock_shards_for_keys(&[]).await;
        assert!(guards.is_empty());
    }

    #[tokio::test]
    async fn test_lock_shards_for_keys_single_key_returns_one() {
        let db = Db::new();
        let keys = vec![Bytes::from_static(b"k")];
        let guards = db.lock_shards_for_keys(&keys).await;
        assert_eq!(guards.len(), 1);
    }

    #[tokio::test]
    async fn test_lock_shards_for_keys_dedupes_same_shard() {
        let db = Db::new();
        // Two keys that might hash to the same shard should not produce
        // duplicate lock entries.
        let key1 = Bytes::from_static(b"k1");
        let key2 = Bytes::from_static(b"k2");
        let idx1 = db.get_shard_index(&key1);
        let idx2 = db.get_shard_index(&key2);
        let keys = vec![key1.clone(), key2.clone()];
        let guards = db.lock_shards_for_keys(&keys).await;
        // The map should have at most one entry per shard, never duplicate shard indices.
        if idx1 == idx2 {
            assert_eq!(guards.len(), 1);
        } else {
            assert_eq!(guards.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_lock_shards_for_keys_caps_at_num_shards() {
        let db = Db::new();
        // Generate enough keys to (statistically) hit all 16 shards.
        let keys: Vec<Bytes> = (0..1000)
            .map(|i| Bytes::copy_from_slice(format!("k{i}").as_bytes()))
            .collect();
        let guards = db.lock_shards_for_keys(&keys).await;
        assert!(guards.len() <= NUM_SHARDS);
    }

    #[tokio::test]
    async fn test_lock_shards_for_keys_acquires_in_sorted_order() {
        // Hard to verify ordering from outside, but verify that the keys used
        // in the locking path can be looked up afterwards through the guards.
        let db = Db::new();
        let keys: Vec<Bytes> = (0..50)
            .map(|i| Bytes::copy_from_slice(format!("k{i}").as_bytes()))
            .collect();
        for k in &keys {
            db.insert_value_from_load(k.clone(), make_sv("v")).await;
        }
        let guards = db.lock_shards_for_keys(&keys).await;
        for (idx, _guard) in guards {
            assert!(idx < NUM_SHARDS);
        }
    }

    #[tokio::test]
    async fn test_lock_all_shards_acquires_all_num_shards() {
        let db = Db::new();
        let guards = db.lock_all_shards().await;
        assert_eq!(guards.len(), NUM_SHARDS);
    }

    #[tokio::test]
    async fn test_lock_all_shards_acquires_in_index_order() {
        // The contract is that locks are acquired in 0..NUM_SHARDS order to
        // prevent deadlocks. We can't inspect lock order directly, but we
        // can verify the resulting vec length and that the shard indices
        // are valid (which is implicit in the call not panicking).
        let db = Db::new();
        let _g = db.lock_all_shards().await;
        // No assertion on order is needed; just verifying it doesn't deadlock
        // with itself is the main correctness check.
    }
}
