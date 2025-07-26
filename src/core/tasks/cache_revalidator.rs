// src/core/tasks/cache_revalidator.rs

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::core::commands::cache::cache_get::revalidate_and_update_cache;
use crate::core::state::ServerState;
use crate::core::state::cache::RevalidationJob;

use crate::core::storage::data_types::DataValue;

/// A task responsible for performing background cache revalidations.
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
                    tokio::spawn(async move {
                        if let Err(e) =
                            revalidate_and_update_cache(state_clone, job.key, job.url, job.variant_hash, None)
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
                    if let Some(expiry) = entry.expiry {
                        let time_left = expiry.saturating_duration_since(Instant::now());

                        if time_left > Duration::ZERO
                            && time_left <= CACHE_REVALIDATOR_PRE_WARM_WINDOW
                        {
                            let key_clone = key.clone();
                            let variants_clone = variants.clone();
                            drop(guard);

                            self.state
                                .cache
                                .trigger_smart_background_revalidation(key_clone, variants_clone)
                                .await;
                            break;
                        }
                    }
                }
            } else {
                self.state.cache.prewarm_keys.write().await.remove(&key);
            }
        }
    }
}
