// src/core/tasks/cache_revalidator.rs
//! Implements the background worker and proactive revalidator tasks for the
//! Intelligent Caching Engine.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::core::commands::cache::cache_get::revalidate_and_update_cache;
use crate::core::state::ServerState;
use crate::core::state::cache::RevalidationJob;
use crate::core::storage::data_types::DataValue;

/// A task responsible for performing background cache revalidations,
/// typically triggered by a stale-while-revalidate (SWR) policy.
pub struct CacheRevalidationWorker {
    pub state: Arc<ServerState>,
    /// Receives revalidation jobs from the main application threads.
    pub rx: mpsc::Receiver<RevalidationJob>,
}

impl CacheRevalidationWorker {
    /// Runs the main loop for the cache revalidation worker.
    pub async fn run(mut self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!("Cache revalidation worker task started.");
        loop {
            tokio::select! {
                Some(job) = self.rx.recv() => {
                    let state_clone = self.state.clone();
                    // Spawn each revalidation job as a separate task to allow for concurrent fetches.
                    tokio::spawn(async move {
                        // The spawned task must acquire its own lock on the relevant shard.
                        let db = state_clone.get_db(0).unwrap();
                        let shard_index = db.get_shard_index(&job.key);
                        let mut guard = db.get_shard(shard_index).entries.lock().await;

                        if let Err(e) =
                            revalidate_and_update_cache(state_clone, job.key, job.url, job.variant_hash, None, &mut guard)
                                .await
                        {
                            warn!("Proactive cache revalidation failed: {}", e);
                        }
                    });
                }
                _ = shutdown_rx.recv() => {
                    info!("Cache revalidation worker shutting down.");
                    return;
                }
            }
        }
    }
}

/// The interval for the proactive cache revalidator task.
const CACHE_REVALIDATOR_INTERVAL: Duration = Duration::from_secs(10);
/// The number of cache keys to sample in each revalidation cycle.
const CACHE_REVALIDATOR_SAMPLE_SIZE: usize = 20;
/// The time window before expiry to trigger a proactive revalidation.
const CACHE_REVALIDATOR_PRE_WARM_WINDOW: Duration = Duration::from_secs(10);

/// A task responsible for proactive cache revalidation (pre-warming).
/// It periodically samples keys marked with a `prewarm` policy and revalidates them
/// just before they expire.
pub struct CacheRevalidator {
    state: Arc<ServerState>,
}

impl CacheRevalidator {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// Runs the main loop for the cache revalidator.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!("Proactive cache revalidator task started.");
        let mut interval = tokio::time::interval(CACHE_REVALIDATOR_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.perform_revalidation_cycle().await;
                }
                _ = shutdown_rx.recv() => {
                    info!("Cache revalidator shutting down.");
                    return;
                }
            }
        }
    }

    /// Performs a single cycle of sampling and revalidating cache keys.
    async fn perform_revalidation_cycle(&self) {
        let db = if let Some(db) = self.state.dbs.first() {
            db
        } else {
            return;
        };

        let prewarm_keys_guard = self.state.cache.prewarm_keys.read().await;
        if prewarm_keys_guard.is_empty() {
            return;
        }

        // Take a small, random sample of keys to check in this cycle.
        let sample: Vec<Bytes> = prewarm_keys_guard
            .iter()
            .take(CACHE_REVALIDATOR_SAMPLE_SIZE)
            .cloned()
            .collect();

        drop(prewarm_keys_guard);

        for key in sample {
            let shard_index = db.get_shard_index(&key);
            let guard = db.get_shard(shard_index).entries.lock().await;

            if let Some(entry) = guard.peek(&key) {
                if let DataValue::HttpCache { variants, .. } = &entry.data {
                    // Check if the entry has an expiry and is within the pre-warm window.
                    if let Some(expiry) = entry.expiry {
                        let time_left = expiry.saturating_duration_since(Instant::now());

                        if time_left > Duration::ZERO
                            && time_left <= CACHE_REVALIDATOR_PRE_WARM_WINDOW
                        {
                            let key_clone = key.clone();
                            let variants_clone = variants.clone();
                            // Drop the lock before calling the async revalidation function
                            // to avoid holding the lock across an await point.
                            drop(guard);

                            self.state
                                .cache
                                .trigger_smart_background_revalidation(key_clone, variants_clone)
                                .await;
                        }
                    }
                }
            } else {
                // The key exists in the prewarm set but not in the database,
                // so it was likely deleted. Remove it from the prewarm set.
                self.state.cache.prewarm_keys.write().await.remove(&key);
            }
        }
    }
}
