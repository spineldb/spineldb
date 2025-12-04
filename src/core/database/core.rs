// src/core/database/core.rs

use super::shard::DbShard;
use super::transaction::TransactionState;
use crate::core::storage::data_types::StoredValue;
use bytes::Bytes;
use dashmap::DashMap;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// The number of shards per database.
pub const NUM_SHARDS: usize = 16;

/// `Db` represents a single database, composed of multiple `DbShard`s.
#[derive(Debug)]
pub struct Db {
    /// The collection of shards that make up this database.
    pub shards: Vec<Arc<DbShard>>,
    /// The state of ongoing transactions, keyed by session ID.
    pub tx_states: Arc<DashMap<u64, TransactionState>>,
}

/// Defines the direction for list push operations.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum PushDirection {
    Left,
    Right,
}

/// Defines the direction for list pop operations.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum PopDirection {
    Left,
    Right,
}

impl Db {
    /// Creates a new, empty `Db` instance.
    pub fn new() -> Self {
        let shards = (0..NUM_SHARDS).map(|_| Arc::new(DbShard::new())).collect();
        Self {
            shards,
            tx_states: Arc::new(DashMap::new()),
        }
    }

    /// Calculates the shard index for a given key using hashing.
    pub fn get_shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }

    /// Returns the total number of keys in the database. O(1) complexity.
    pub fn get_key_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.key_count.load(Ordering::Relaxed))
            .sum()
    }

    /// Gets a list of keys belonging to a specific cluster slot using the slot index.
    pub async fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        let mut keys_in_slot = Vec::with_capacity(count);

        // Iterate through all shards, as keys for a single slot can be distributed
        // across shards based on their primary hash (not the slot hash).
        for shard in &self.shards {
            if keys_in_slot.len() >= count {
                break;
            }

            let guard = shard.entries.lock().await;

            // Use the secondary index for an efficient O(1) lookup within the shard.
            if let Some(keys_for_slot) = guard.slot_index.get(&slot) {
                for key in keys_for_slot {
                    // It's still necessary to check for expiration.
                    if guard.peek(key).is_some_and(|v| !v.is_expired()) {
                        keys_in_slot.push(key.clone());
                        if keys_in_slot.len() >= count {
                            break;
                        }
                    }
                }
            }
        }
        keys_in_slot
    }

    /// Gets a reference to a shard by its index.
    pub fn get_shard(&self, index: usize) -> &Arc<DbShard> {
        &self.shards[index]
    }

    /// Inserts a value during a data loading process (SPLDB/AOF).
    pub async fn insert_value_from_load(&self, key: Bytes, value: StoredValue) {
        let shard_index = self.get_shard_index(&key);
        let mut guard = self.shards[shard_index].entries.lock().await;
        guard.put(key, value);
    }

    /// Calculates the total memory used by this database across all shards.
    pub fn get_current_memory(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.current_memory.load(Ordering::Relaxed))
            .sum()
    }

    /// Collects all key-value pairs for synchronization.
    pub async fn get_all_kvs_for_sync(&self) -> Vec<(Bytes, StoredValue)> {
        let all_guards = self.lock_all_shards().await;
        let mut all_kvs = Vec::new();
        for mut guard in all_guards {
            all_kvs.extend(
                guard
                    .iter_mut()
                    .filter(|(_, value)| !value.is_expired())
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        all_kvs
    }

    /// Gets a random sample of keys that might be expired for active deletion.
    pub async fn get_expired_sample_keys(&self, sample_size: usize) -> Vec<Bytes> {
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let mut expired_keys = Vec::with_capacity(sample_size);
        for _ in 0..sample_size {
            let shard_index = rng.gen_range(0..NUM_SHARDS);
            let guard = self.shards[shard_index].entries.lock().await;
            if let Some((key, _)) = guard
                .iter()
                .filter(|(_, v)| v.is_expired())
                .choose(&mut rng)
            {
                expired_keys.push(key.clone());
            }
        }
        expired_keys
    }

    /// Gets a random sample of keys from the database, regardless of expiry.
    pub async fn get_random_keys(&self, sample_size: usize) -> Vec<Bytes> {
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let mut keys = Vec::with_capacity(sample_size);
        for _ in 0..sample_size {
            let shard_index = rng.gen_range(0..NUM_SHARDS);
            let guard = self.shards[shard_index].entries.lock().await;
            if let Some((key, _)) = guard.iter().choose(&mut rng) {
                keys.push(key.clone());
            }
        }
        keys
    }

    /// Deletes a list of keys from the database.
    pub async fn del(&self, keys: &[Bytes]) -> usize {
        if keys.is_empty() {
            return 0;
        }
        if keys.len() == 1 {
            let key = &keys[0];
            let shard_index = self.get_shard_index(key);
            let mut guard = self.shards[shard_index].entries.lock().await;
            return if guard.pop(key).is_some() { 1 } else { 0 };
        }
        let mut locks = self.lock_shards_for_keys(keys).await;
        let mut count = 0;
        for key in keys {
            let shard_index = self.get_shard_index(key);
            if let Some(guard) = locks.get_mut(&shard_index)
                && guard.pop(key).is_some()
            {
                count += 1;
            }
        }
        count
    }

    /// Performs a SCAN-like operation on the keyspace.
    pub async fn scan_keys(&self, cursor: u64, count: usize) -> (u64, Vec<Bytes>) {
        let (mut current_shard_idx, mut internal_cursor) =
            crate::core::commands::scan::helpers::decode_scan_cursor(cursor);
        let mut result_keys = Vec::with_capacity(count);

        'outer: while current_shard_idx < NUM_SHARDS {
            let shard = self.get_shard(current_shard_idx);
            let guard = shard.entries.lock().await;

            let starting_point = internal_cursor;
            internal_cursor = 0;

            // Iterate directly on the LRU cache iterator to avoid collecting all keys into a vector.
            for (i, (key, _value)) in guard.iter().enumerate().skip(starting_point) {
                // The value from the iterator might be stale, so we peek to get the latest state
                // and check for expiration.
                if guard.peek(key).is_some_and(|value| !value.is_expired()) {
                    result_keys.push(key.clone());
                }

                if result_keys.len() >= count {
                    internal_cursor = i + 1; // Save the position for the next scan call.
                    break 'outer;
                }
            }
            current_shard_idx += 1;
        }

        let new_cursor = if current_shard_idx >= NUM_SHARDS {
            0
        } else {
            crate::core::commands::scan::helpers::encode_scan_cursor(
                current_shard_idx,
                internal_cursor,
            )
        };

        (new_cursor, result_keys)
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            tx_states: self.tx_states.clone(),
        }
    }
}
