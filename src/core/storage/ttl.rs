// src/core/storage/ttl.rs

//! Implements the active, sampling-based TTL expiration manager.

use crate::core::database::Db;
use crate::core::metrics;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, info};

/// The interval at which the TTL manager checks for expired keys.
const TTL_CHECK_INTERVAL: Duration = Duration::from_millis(100);
/// The number of keys to sample from each database in each cycle.
const TTL_SAMPLE_SIZE: usize = 20;
/// The percentage threshold of expired keys in a sample that triggers
/// an immediate re-run of the check for that database.
const TTL_EXPIRED_THRESHOLD_PERCENT: u32 = 25;

/// `TtlManager` is a background task that actively expires keys to prevent
/// memory build-up from expired data that is never accessed again.
pub struct TtlManager {
    dbs: Vec<Arc<Db>>,
}

impl TtlManager {
    /// Creates a new `TtlManager` for the given set of databases.
    pub fn new(dbs: Vec<Arc<Db>>) -> Self {
        Self { dbs }
    }

    /// Runs the main loop for the TTL expiration manager.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!("TTL expiration manager started (active, sampling-based).");
        let mut interval = tokio::time::interval(TTL_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.purge_expired_keys_with_sampling().await;
                }
                _ = shutdown_rx.recv() => {
                    info!("TTL expiration manager shutting down.");
                    return;
                }
            }
        }
    }

    /// Performs one cycle of the active expiration algorithm for all databases.
    ///
    /// The algorithm is based on Redis's active expiration:
    /// 1. For each database, a random sample of keys with expirations is taken.
    /// 2. Expired keys from the sample are deleted.
    /// 3. If the percentage of expired keys in the sample is above a threshold,
    ///    the cycle is repeated immediately for that database.
    /// 4. This process continues until the percentage of expired keys drops
    ///    below the threshold or the time limit for the cycle is reached.
    async fn purge_expired_keys_with_sampling(&self) {
        for db in &self.dbs {
            loop {
                // Get a random sample of keys that might be expired.
                let expired_in_sample = db.get_expired_sample_keys(TTL_SAMPLE_SIZE).await;

                if expired_in_sample.is_empty() {
                    break;
                }

                // Delete the expired keys found in the sample.
                let expired_count = db.del(&expired_in_sample).await;
                if expired_count > 0 {
                    metrics::EXPIRED_KEYS_TOTAL.inc_by(expired_count as f64);
                    debug!(
                        "Purged {} expired keys from sample in a database.",
                        expired_count
                    );
                }

                // If the sample was not full, we've likely checked most of the expired keys.
                if expired_in_sample.len() < TTL_SAMPLE_SIZE {
                    break;
                }

                // If a high percentage of the sample was expired, re-run the cycle immediately.
                let expired_percentage = (expired_count * 100 / TTL_SAMPLE_SIZE) as u32;
                if expired_percentage < TTL_EXPIRED_THRESHOLD_PERCENT {
                    break;
                } else {
                    debug!(
                        "Expired keys percentage ({:.1}%) is high, re-running purge cycle immediately for this database.",
                        expired_percentage
                    );
                }
            }
        }
    }
}
