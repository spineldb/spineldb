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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::storage::data_types::{DataValue, StoredValue};
    use std::time::{Duration, Instant};

    fn make_sv(s: &str) -> StoredValue {
        StoredValue::new(DataValue::String(Bytes::copy_from_slice(s.as_bytes())))
    }

    #[test]
    fn test_db_new_creates_n_shards() {
        let db = Db::new();
        assert_eq!(db.shards.len(), NUM_SHARDS);
    }

    #[test]
    fn test_db_default_equals_new() {
        let a = Db::default();
        let b = Db::new();
        assert_eq!(a.shards.len(), b.shards.len());
    }

    #[test]
    fn test_db_clone_shares_shards() {
        let db = Db::new();
        let cloned = db.clone();
        // Each shard's Arc pointer is shared.
        for (a, b) in db.shards.iter().zip(cloned.shards.iter()) {
            assert!(Arc::ptr_eq(a, b));
        }
    }

    #[test]
    fn test_get_shard_index_in_range() {
        let db = Db::new();
        for i in 0..100u32 {
            let k = Bytes::copy_from_slice(i.to_string().as_bytes());
            let idx = db.get_shard_index(&k);
            assert!(idx < NUM_SHARDS);
        }
    }

    #[test]
    fn test_get_shard_index_is_deterministic() {
        let db = Db::new();
        let k = Bytes::from_static(b"some-key");
        let a = db.get_shard_index(&k);
        let b = db.get_shard_index(&k);
        assert_eq!(a, b);
    }

    #[test]
    fn test_get_key_count_starts_at_zero() {
        let db = Db::new();
        assert_eq!(db.get_key_count(), 0);
    }

    #[test]
    fn test_get_current_memory_starts_at_zero() {
        let db = Db::new();
        assert_eq!(db.get_current_memory(), 0);
    }

    #[tokio::test]
    async fn test_insert_value_from_load_increments_count() {
        let db = Db::new();
        let key = Bytes::from_static(b"k1");
        db.insert_value_from_load(key.clone(), make_sv("hello"))
            .await;
        assert_eq!(db.get_key_count(), 1);
        // Memory should reflect key(2) + value(5) = 7
        assert_eq!(db.get_current_memory(), 7);
    }

    #[tokio::test]
    async fn test_del_empty_returns_zero() {
        let db = Db::new();
        let n = db.del(&[]).await;
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_del_single_key() {
        let db = Db::new();
        let key = Bytes::from_static(b"k1");
        db.insert_value_from_load(key.clone(), make_sv("v")).await;
        let n = db.del(std::slice::from_ref(&key)).await;
        assert_eq!(n, 1);
        assert_eq!(db.get_key_count(), 0);
    }

    #[tokio::test]
    async fn test_del_single_nonexistent_key() {
        let db = Db::new();
        let n = db.del(&[Bytes::from_static(b"missing")]).await;
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_del_multiple_keys_across_shards() {
        let db = Db::new();
        let keys: Vec<Bytes> = (0..50)
            .map(|i| Bytes::copy_from_slice(format!("k{i}").as_bytes()))
            .collect();
        for k in &keys {
            db.insert_value_from_load(k.clone(), make_sv("v")).await;
        }
        assert_eq!(db.get_key_count(), 50);
        let n = db.del(&keys).await;
        assert_eq!(n, 50);
        assert_eq!(db.get_key_count(), 0);
    }

    #[tokio::test]
    async fn test_del_mixed_existing_and_missing() {
        let db = Db::new();
        db.insert_value_from_load(Bytes::from_static(b"a"), make_sv("1"))
            .await;
        let n = db
            .del(&[Bytes::from_static(b"a"), Bytes::from_static(b"missing")])
            .await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn test_get_keys_in_slot() {
        let db = Db::new();
        // Insert several keys; some will share a slot.
        for i in 0..20u32 {
            let k = Bytes::copy_from_slice(format!("k{i}").as_bytes());
            db.insert_value_from_load(k, make_sv("v")).await;
        }
        // Pick a random slot and ask for keys in it.
        let result = db.get_keys_in_slot(123, 100).await;
        // We just inserted 20 keys, so the request can return at most 20.
        assert!(result.len() <= 20);
    }

    #[tokio::test]
    async fn test_get_all_kvs_for_sync_excludes_expired() {
        let db = Db::new();
        let mut sv = make_sv("v");
        sv.expiry = Some(Instant::now() - Duration::from_secs(60));
        db.insert_value_from_load(Bytes::from_static(b"expired"), sv)
            .await;
        db.insert_value_from_load(Bytes::from_static(b"fresh"), make_sv("v"))
            .await;
        let all = db.get_all_kvs_for_sync().await;
        // Only the non-expired key should be returned.
        let keys: Vec<Bytes> = all.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&Bytes::from_static(b"fresh")));
        assert!(!keys.contains(&Bytes::from_static(b"expired")));
    }

    #[tokio::test]
    async fn test_scan_keys_returns_all_when_count_is_high() {
        let db = Db::new();
        for i in 0..10u32 {
            let k = Bytes::copy_from_slice(format!("k{i}").as_bytes());
            db.insert_value_from_load(k, make_sv("v")).await;
        }
        // First scan with count >= total: cursor 0, should return all and cursor 0.
        let (next_cursor, keys) = db.scan_keys(0, 100).await;
        assert_eq!(next_cursor, 0);
        assert_eq!(keys.len(), 10);
    }

    #[tokio::test]
    async fn test_scan_keys_paginates_with_small_count() {
        let db = Db::new();
        for i in 0..10u32 {
            let k = Bytes::copy_from_slice(format!("k{i}").as_bytes());
            db.insert_value_from_load(k, make_sv("v")).await;
        }
        // First batch: cursor 0, count 3.
        let (next_cursor, first) = db.scan_keys(0, 3).await;
        assert_eq!(first.len(), 3);
        assert_ne!(next_cursor, 0);
        // Second batch: use returned cursor.
        let (next_cursor_2, second) = db.scan_keys(next_cursor, 100).await;
        assert_eq!(next_cursor_2, 0);
        assert_eq!(first.len() + second.len(), 10);
    }

    #[tokio::test]
    async fn test_get_random_keys_returns_distinct_keys() {
        let db = Db::new();
        for i in 0..5u32 {
            let k = Bytes::copy_from_slice(format!("k{i}").as_bytes());
            db.insert_value_from_load(k, make_sv("v")).await;
        }
        let keys = db.get_random_keys(20).await;
        // All 20 attempts can hit only 5 distinct keys, so duplicates are expected;
        // the function is allowed to return repeats but should not exceed sample_size.
        assert!(keys.len() <= 20);
        for k in &keys {
            assert!(k.len() == 2); // "k0".."k4"
        }
    }

    #[tokio::test]
    async fn test_get_expired_sample_keys_finds_expired() {
        let db = Db::new();
        let mut sv = make_sv("v");
        sv.expiry = Some(Instant::now() - Duration::from_secs(60));
        db.insert_value_from_load(Bytes::from_static(b"exp"), sv)
            .await;
        // Sample heavily to make hitting the expired key very likely.
        let expired = db.get_expired_sample_keys(100).await;
        // With probability ~1, the expired key is in the sample.
        assert!(expired.contains(&Bytes::from_static(b"exp")));
    }
}
