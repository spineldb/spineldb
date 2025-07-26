// src/core/storage/db/shard.rs

//! Defines the `DbShard` and `ShardCache` structs, which form the fundamental
//! storage units within a `Db`.

use crate::core::storage::data_types::StoredValue;
use bytes::Bytes;
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

/// Default capacity for the LRU cache within each shard.
const DEFAULT_SHARD_LRU_CAPACITY: usize = 250_000;
/// Default pre-allocated capacity for the tag-to-keys index.
const DEFAULT_TAG_INDEX_CAPACITY: usize = 10_000;

/// A `DbShard` is a single, concurrent slice of the database.
/// It contains a mutex-guarded `ShardCache` and atomic counters for performance.
#[derive(Debug)]
pub struct DbShard {
    /// The actual key-value store, protected by a Mutex for thread-safe access.
    pub entries: Mutex<ShardCache>,
    /// An atomic counter for the total memory used by this shard in bytes.
    pub current_memory: Arc<AtomicUsize>,
    /// An atomic counter for the total number of keys in this shard.
    pub key_count: Arc<AtomicUsize>,
}

/// A `ShardCache` wraps the `LruCache` and manages associated metadata like
/// memory accounting, key counting, and the tag-to-keys index.
#[derive(Debug)]
pub struct ShardCache {
    /// The underlying key-value store with LRU behavior.
    store: LruCache<Bytes, StoredValue>,
    /// An index mapping tags to a set of keys associated with that tag.
    pub tag_index: HashMap<Bytes, HashSet<Bytes>>,
    /// A shared atomic counter for the shard's total memory usage.
    memory_counter: Arc<AtomicUsize>,
    /// A shared atomic counter for the shard's total key count.
    key_counter: Arc<AtomicUsize>,
}

impl DbShard {
    /// Creates a new, empty `DbShard`.
    pub(super) fn new() -> Self {
        let lru_capacity = NonZeroUsize::new(DEFAULT_SHARD_LRU_CAPACITY).unwrap();
        let current_memory = Arc::new(AtomicUsize::new(0));
        let key_count = Arc::new(AtomicUsize::new(0));
        Self {
            entries: Mutex::new(ShardCache::new(
                lru_capacity,
                current_memory.clone(),
                key_count.clone(),
            )),
            current_memory,
            key_count,
        }
    }

    /// Atomically updates the shard's memory counter by a given delta.
    pub fn update_memory(&self, diff: isize) {
        if diff > 0 {
            self.current_memory
                .fetch_add(diff as usize, Ordering::Relaxed);
        } else {
            self.current_memory
                .fetch_sub((-diff) as usize, Ordering::Relaxed);
        }
    }
}

impl ShardCache {
    /// Creates a new, empty `ShardCache`.
    fn new(
        capacity: NonZeroUsize,
        memory_counter: Arc<AtomicUsize>,
        key_counter: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            store: LruCache::new(capacity),
            tag_index: HashMap::with_capacity(DEFAULT_TAG_INDEX_CAPACITY),
            memory_counter,
            key_counter,
        }
    }

    /// Puts a key-value pair into the cache, handling all memory and key count accounting.
    /// It returns the old value if the key already existed.
    pub fn put(&mut self, key: Bytes, mut value: StoredValue) -> Option<StoredValue> {
        // Always calculate the accurate size of the data payload before insertion.
        value.size = value.data.memory_usage();
        let new_item_mem = key.len() + value.size;

        let old_value = self.store.put(key.clone(), value);

        if let Some(ref old) = old_value {
            // Key existed, calculate the memory difference.
            let old_item_mem = key.len() + old.size;
            let mem_diff = new_item_mem as isize - old_item_mem as isize;
            self.update_memory(mem_diff);
            // Tags from the old value must be cleared before adding new ones.
            self.remove_key_from_tags(&key);
        } else {
            // This is a new key.
            self.update_memory(new_item_mem as isize);
            self.key_counter.fetch_add(1, Ordering::Relaxed);
        }
        old_value
    }

    /// Removes a key from the cache, returning the value if the key was present.
    /// This method handles all necessary memory and key count decrements.
    pub fn pop(&mut self, key: &Bytes) -> Option<StoredValue> {
        if let Some(popped_value) = self.store.pop(key) {
            let mem_to_free = key.len() + popped_value.size;
            self.update_memory(-(mem_to_free as isize));
            self.key_counter.fetch_sub(1, Ordering::Relaxed);
            self.remove_key_from_tags(key);
            Some(popped_value)
        } else {
            None
        }
    }

    /// Removes and returns the least recently used item from the cache.
    pub fn pop_lru(&mut self) -> Option<(Bytes, StoredValue)> {
        if let Some((k, v)) = self.store.pop_lru() {
            let mem_to_free = k.len() + v.size;
            self.update_memory(-(mem_to_free as isize));
            self.key_counter.fetch_sub(1, Ordering::Relaxed);
            self.remove_key_from_tags(&k);
            Some((k, v))
        } else {
            None
        }
    }

    /// Updates the global atomic memory counter for this shard.
    fn update_memory(&self, diff: isize) {
        if diff > 0 {
            self.memory_counter
                .fetch_add(diff as usize, Ordering::Relaxed);
        } else {
            self.memory_counter
                .fetch_sub((-diff) as usize, Ordering::Relaxed);
        }
    }

    /// Clears all entries from the shard, resetting memory and key counters.
    pub fn clear(&mut self) {
        if self.store.is_empty() {
            return;
        }
        self.store.clear();
        self.tag_index.clear();
        self.memory_counter.store(0, Ordering::Relaxed);
        self.key_counter.store(0, Ordering::Relaxed);
    }

    /// Gets a mutable reference to a value, inserting a default if it doesn't exist.
    pub fn get_or_insert_with_mut<F>(&mut self, key: Bytes, f: F) -> &mut StoredValue
    where
        F: FnOnce() -> StoredValue,
    {
        if self.store.get(&key).is_none() {
            let new_value = f();
            self.put(key.clone(), new_value);
        }
        self.store.get_mut(&key).unwrap()
    }

    /// Gets a mutable reference to a value, updating its LFU/LRU metadata.
    pub fn get_mut(&mut self, key: &Bytes) -> Option<&mut StoredValue> {
        if let Some(entry) = self.store.get_mut(key) {
            entry.update_lfu();
            return Some(entry);
        }
        None
    }

    /// Gets an immutable reference to a value, updating its LFU/LRU metadata.
    pub fn get(&mut self, key: &Bytes) -> Option<&StoredValue> {
        if let Some(entry) = self.store.get_mut(key) {
            entry.update_lfu();
        }
        self.store.get(key)
    }

    /// Gets an immutable reference to a value without updating its LFU/LRU metadata.
    pub fn peek(&self, key: &Bytes) -> Option<&StoredValue> {
        self.store.peek(key)
    }

    /// Returns an iterator over the key-value pairs in the shard.
    pub fn iter(&self) -> lru::Iter<'_, Bytes, StoredValue> {
        self.store.iter()
    }

    /// Returns a mutable iterator over the key-value pairs in the shard.
    pub fn iter_mut(&mut self) -> lru::IterMut<'_, Bytes, StoredValue> {
        self.store.iter_mut()
    }

    /// Removes a key from all tag indexes it may be a part of.
    pub fn remove_key_from_tags(&mut self, key: &Bytes) {
        self.tag_index.values_mut().for_each(|keys| {
            keys.remove(key);
        });
        // Prune empty tags from the index to save memory.
        self.tag_index.retain(|_, keys| !keys.is_empty());
    }

    /// Associates a key with a given set of tags in the index.
    pub fn add_tags_for_key(&mut self, key: Bytes, tags: &[Bytes]) {
        if tags.is_empty() {
            return;
        }
        for tag in tags {
            self.tag_index
                .entry(tag.clone())
                .or_default()
                .insert(key.clone());
        }
    }

    // Returns all tags associated with a given key.
    pub fn get_tags_for_key(&self, key: &Bytes) -> Vec<Bytes> {
        self.tag_index
            .iter()
            .filter_map(|(tag, keys)| {
                if keys.contains(key) {
                    Some(tag.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}
