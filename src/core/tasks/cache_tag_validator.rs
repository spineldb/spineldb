// src/core/tasks/cache_tag_validator.rs

use crate::core::state::ServerState;
use crate::core::storage::data_types::DataValue;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, info};

const VALIDATOR_INTERVAL: Duration = Duration::from_secs(1);
const VALIDATOR_SAMPLE_SIZE: usize = 20;

pub struct CacheTagValidatorTask {
    state: Arc<ServerState>,
}

impl CacheTagValidatorTask {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        if self.state.cluster.is_none() {
            info!("Cache tag validator runs only in cluster mode. Task will not start.");
            return;
        }
        info!("Cache tag validator task started.");
        let mut interval = tokio::time::interval(VALIDATOR_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.perform_validation_cycle().await;
                }
                _ = shutdown_rx.recv() => {
                    info!("Cache tag validator task shutting down.");
                    return;
                }
            }
        }
    }

    async fn perform_validation_cycle(&self) {
        let db = self.state.get_db(0).unwrap();
        let sample = db.get_random_keys(VALIDATOR_SAMPLE_SIZE).await;
        if sample.is_empty() {
            return;
        }

        let mut keys_to_delete = Vec::new();
        for key in sample {
            let shard_index = db.get_shard_index(&key);
            let guard = db.get_shard(shard_index).entries.lock().await;

            if let Some(entry) = guard.peek(&key)
                && let DataValue::HttpCache { tags_epoch, .. } = &entry.data
            {
                let tags_for_key = guard.get_tags_for_key(&key);
                for tag in tags_for_key {
                    if let Some(latest_purge_epoch) = self.state.cache.tag_purge_epochs.get(&tag)
                        && *tags_epoch < *latest_purge_epoch.value()
                    {
                        debug!(
                            "Found stale cache entry '{}' due to tag '{}' purge race condition. Deleting.",
                            String::from_utf8_lossy(&key),
                            String::from_utf8_lossy(&tag)
                        );
                        keys_to_delete.push(key.clone());
                        break; // Key only needs to be deleted once
                    }
                }
            }
        }

        if !keys_to_delete.is_empty() {
            db.del(&keys_to_delete).await;
        }
    }
}
