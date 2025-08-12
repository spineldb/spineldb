// src/core/storage/db/eviction.rs

//! Implements the memory eviction logic for a `Db` instance. This is triggered
//! by the background EvictionManager when `maxmemory` is reached.

use super::core::{Db, NUM_SHARDS};
use crate::config::EvictionPolicy;
use crate::core::state::ServerState;
use crate::core::storage::data_types::{DataValue, LfuInfo, StoredValue};
use bytes::Bytes;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

impl Db {
    /// Tries to evict a single key based on the configured policy.
    /// Returns `true` if a key was successfully evicted.
    pub async fn evict_one_key(&self, state: &Arc<ServerState>) -> bool {
        let policy = state.config.lock().await.maxmemory_policy;

        if policy == EvictionPolicy::NoEviction {
            return false;
        }

        // Check if the DB is empty.
        if self.get_key_count() == 0 {
            return false;
        }

        // Dispatch to the specific eviction policy implementation.
        let evicted_key = match policy {
            EvictionPolicy::AllkeysLru => self.evict_lru(state).await,
            EvictionPolicy::VolatileLru => self.evict_volatile_lru(state).await,
            EvictionPolicy::AllkeysRandom => self.evict_random_key(state, false).await,
            EvictionPolicy::VolatileRandom => self.evict_random_key(state, true).await,
            EvictionPolicy::VolatileTtl => self.evict_volatile_ttl(state).await,
            EvictionPolicy::AllkeysLfu => self.evict_lfu(state, false).await,
            EvictionPolicy::VolatileLfu => self.evict_lfu(state, true).await,
            EvictionPolicy::NoEviction => return false,
        };

        // If the primary policy failed to find a key, fallback to allkeys-random.
        // This prevents getting stuck if, for example, `volatile-lru` is set but no keys have TTLs.
        if evicted_key.is_none() {
            warn!(
                "Could not find a key to evict with policy '{:?}'. Falling back to allkeys-random.",
                policy
            );
            return self.evict_random_key(state, false).await.is_some();
        }

        evicted_key.is_some()
    }

    /// A helper to check if an evicted value was a cache item and, if so,
    /// increments the global cache eviction statistic.
    fn handle_cache_eviction_stat(state: &Arc<ServerState>, value: &StoredValue) {
        if matches!(value.data, DataValue::HttpCache { .. }) {
            state.cache.increment_evictions();
        }
    }

    /// Evicts the least recently used key from a random shard. Returns the key if successful.
    async fn evict_lru(&self, state: &Arc<ServerState>) -> Option<Bytes> {
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let shard_index = rng.gen_range(0..NUM_SHARDS);
        let mut guard = self.get_shard(shard_index).entries.lock().await;

        if let Some((key, value)) = guard.pop_lru() {
            Self::handle_cache_eviction_stat(state, &value);
            debug!(
                "Evicted LRU key '{}' from shard {}",
                String::from_utf8_lossy(&key),
                shard_index
            );
            Some(key)
        } else {
            None
        }
    }

    /// Evicts a random key from a random shard. Returns the key if successful.
    async fn evict_random_key(
        &self,
        state: &Arc<ServerState>,
        volatile_only: bool,
    ) -> Option<Bytes> {
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let shard_index = rng.gen_range(0..NUM_SHARDS);
        let mut guard = self.get_shard(shard_index).entries.lock().await;

        let key_to_evict = guard
            .iter()
            .filter(|(_, v)| !volatile_only || v.expiry.is_some())
            .map(|(k, _)| k.clone())
            .choose(&mut rng);

        if let Some(key) = key_to_evict
            && let Some(value) = guard.pop(&key)
        {
            Self::handle_cache_eviction_stat(state, &value);
            debug!(
                "Evicted RANDOM key '{}' (volatile_only: {}) from shard {}.",
                String::from_utf8_lossy(&key),
                volatile_only,
                shard_index
            );
            return Some(key);
        }
        None
    }

    /// Evicts the least recently used key that has an expiry set. Returns the key if successful.
    async fn evict_volatile_lru(&self, state: &Arc<ServerState>) -> Option<Bytes> {
        if let Some((key, shard_index)) = self.find_volatile_lru_candidate().await {
            let mut guard = self.get_shard(shard_index).entries.lock().await;
            if let Some(value) = guard.pop(&key) {
                Self::handle_cache_eviction_stat(state, &value);
                debug!(
                    "Evicted VOLATILE-LRU key '{}' from shard {}.",
                    String::from_utf8_lossy(&key),
                    shard_index
                );
                return Some(key);
            }
        }
        None
    }

    /// Helper for volatile-lru: samples shards to find a candidate.
    async fn find_volatile_lru_candidate(&self) -> Option<(Bytes, usize)> {
        const SAMPLE_SIZE: usize = 5;
        let mut rng = rand::rngs::SmallRng::from_entropy();
        for _ in 0..SAMPLE_SIZE {
            let shard_index = rng.gen_range(0..NUM_SHARDS);
            let guard = self.get_shard(shard_index).entries.lock().await;
            // Iterate from the back (least recently used) and find the first with an expiry.
            if let Some((key, _)) = guard.iter().rev().find(|(_, v)| v.expiry.is_some()) {
                return Some((key.clone(), shard_index));
            }
        }
        None
    }

    /// Evicts the key with the nearest expiration time. Returns the key if successful.
    async fn evict_volatile_ttl(&self, state: &Arc<ServerState>) -> Option<Bytes> {
        const SAMPLE_SIZE: usize = 5;
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let mut best_candidate: Option<(Bytes, Instant, usize)> = None;

        for _ in 0..SAMPLE_SIZE {
            let shard_index = rng.gen_range(0..NUM_SHARDS);
            let guard = self.get_shard(shard_index).entries.lock().await;

            if let Some((key, val)) = guard
                .iter()
                .filter(|(_, v)| v.expiry.is_some())
                .min_by_key(|(_, v)| v.expiry.unwrap())
                && (best_candidate.is_none()
                    || val.expiry.unwrap() < best_candidate.as_ref().unwrap().1)
            {
                best_candidate = Some((key.clone(), val.expiry.unwrap(), shard_index));
            }
        }

        if let Some((key_to_evict, _, shard_index)) = best_candidate {
            let mut guard = self.get_shard(shard_index).entries.lock().await;
            if let Some(value) = guard.pop(&key_to_evict) {
                Self::handle_cache_eviction_stat(state, &value);
                debug!(
                    "Evicted VOLATILE-TTL key '{}' from shard {}.",
                    String::from_utf8_lossy(&key_to_evict),
                    shard_index
                );
                return Some(key_to_evict);
            }
        }
        None
    }

    /// Evicts a key based on the LFU (Least Frequently Used) policy. Returns the key if successful.
    async fn evict_lfu(&self, state: &Arc<ServerState>, volatile_only: bool) -> Option<Bytes> {
        const SAMPLE_SIZE: usize = 5;
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let mut best_candidate: Option<(Bytes, LfuInfo, usize)> = None;

        for _ in 0..(SAMPLE_SIZE * 2) {
            let shard_index = rng.gen_range(0..NUM_SHARDS);
            let guard = self.get_shard(shard_index).entries.lock().await;

            if let Some((key, val)) = guard.iter().choose(&mut rng) {
                if volatile_only && val.expiry.is_none() {
                    continue;
                }
                if best_candidate.is_none()
                    || val.lfu.counter < best_candidate.as_ref().unwrap().1.counter
                {
                    best_candidate = Some((key.clone(), val.lfu, shard_index));
                }
            }
        }

        if let Some((key_to_evict, _, shard_index)) = best_candidate {
            let mut guard = self.get_shard(shard_index).entries.lock().await;
            if let Some(value) = guard.pop(&key_to_evict) {
                Self::handle_cache_eviction_stat(state, &value);
                debug!(
                    "Evicted LFU key '{}' (volatile_only: {}) from shard {}.",
                    String::from_utf8_lossy(&key_to_evict),
                    volatile_only,
                    shard_index
                );
                return Some(key_to_evict);
            }
        }
        None
    }
}
