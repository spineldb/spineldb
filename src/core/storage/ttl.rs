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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::database::Db;
    use crate::core::storage::data_types::{DataValue, StoredValue};
    use bytes::Bytes;
    use std::time::Instant;

    fn make_expired_sv(value: &str) -> StoredValue {
        let mut sv = StoredValue::new(DataValue::String(Bytes::copy_from_slice(value.as_bytes())));
        sv.expiry = Some(Instant::now() - Duration::from_secs(60));
        sv
    }

    fn make_fresh_sv(value: &str) -> StoredValue {
        let mut sv = StoredValue::new(DataValue::String(Bytes::copy_from_slice(value.as_bytes())));
        sv.expiry = Some(Instant::now() + Duration::from_secs(3600));
        sv
    }

    #[test]
    fn test_new_stores_databases() {
        let db = Arc::new(Db::new());
        let mgr = TtlManager::new(vec![db.clone()]);
        // We can't observe dbs directly, but the constructor must accept the list.
        // This is a smoke test that the struct is constructible.
        let _ = mgr;
    }

    #[test]
    fn test_new_with_empty_list() {
        let mgr = TtlManager::new(vec![]);
        let _ = mgr;
    }

    #[tokio::test]
    async fn test_purge_on_empty_db_is_a_noop() {
        let db = Arc::new(Db::new());
        let mgr = TtlManager::new(vec![db.clone()]);
        mgr.purge_expired_keys_with_sampling().await;
        assert_eq!(db.get_key_count(), 0);
    }

    #[tokio::test]
    async fn test_purge_removes_expired_keys() {
        let db = Arc::new(Db::new());
        // Insert 200 expired keys with varied prefixes so they spread across
        // all 16 shards; the random sampler must reliably find them.
        for i in 0..200 {
            let key = Bytes::copy_from_slice(format!("exp:{i}").as_bytes());
            db.insert_value_from_load(key, make_expired_sv("v")).await;
        }
        assert_eq!(db.get_key_count(), 200);

        let mgr = TtlManager::new(vec![db.clone()]);
        // Run multiple cycles to account for the probabilistic sampler.
        for _ in 0..32 {
            mgr.purge_expired_keys_with_sampling().await;
            if db.get_key_count() == 0 {
                break;
            }
        }
        assert_eq!(db.get_key_count(), 0);
    }

    #[tokio::test]
    async fn test_purge_keeps_fresh_keys() {
        let db = Arc::new(Db::new());
        // 100 fresh + 100 expired, all spread across shards.
        for i in 0..100 {
            let key = Bytes::copy_from_slice(format!("fresh:{i}").as_bytes());
            db.insert_value_from_load(key, make_fresh_sv("v")).await;
        }
        for i in 0..100 {
            let key = Bytes::copy_from_slice(format!("exp:{i}").as_bytes());
            db.insert_value_from_load(key, make_expired_sv("v")).await;
        }
        assert_eq!(db.get_key_count(), 200);

        let mgr = TtlManager::new(vec![db.clone()]);
        // Run a fixed number of cycles — far more than needed for 200 keys
        // spread across 16 shards.
        for _ in 0..64 {
            mgr.purge_expired_keys_with_sampling().await;
        }
        // All expired should be gone; all 100 fresh should remain.
        assert_eq!(db.get_key_count(), 100);
    }

    #[tokio::test]
    async fn test_purge_iterates_all_databases() {
        let db1 = Arc::new(Db::new());
        let db2 = Arc::new(Db::new());
        for i in 0..200 {
            let k1 = Bytes::copy_from_slice(format!("a:{i}").as_bytes());
            let k2 = Bytes::copy_from_slice(format!("b:{i}").as_bytes());
            db1.insert_value_from_load(k1, make_expired_sv("v")).await;
            db2.insert_value_from_load(k2, make_expired_sv("v")).await;
        }
        assert_eq!(db1.get_key_count(), 200);
        assert_eq!(db2.get_key_count(), 200);

        let mgr = TtlManager::new(vec![db1.clone(), db2.clone()]);
        for _ in 0..32 {
            mgr.purge_expired_keys_with_sampling().await;
            if db1.get_key_count() == 0 && db2.get_key_count() == 0 {
                break;
            }
        }
        assert_eq!(db1.get_key_count(), 0);
        assert_eq!(db2.get_key_count(), 0);
    }

    #[tokio::test]
    async fn test_purge_with_no_expired_keys_exits_immediately() {
        let db = Arc::new(Db::new());
        for i in 0..10 {
            let k = Bytes::copy_from_slice(format!("k:{i}").as_bytes());
            db.insert_value_from_load(k, make_fresh_sv("v")).await;
        }
        let mgr = TtlManager::new(vec![db.clone()]);
        mgr.purge_expired_keys_with_sampling().await;
        // All keys remain.
        assert_eq!(db.get_key_count(), 10);
    }

    #[tokio::test]
    async fn test_purge_with_many_expired_keys_clears_them() {
        // More keys than the sample size to exercise the repeat-while-expired loop.
        let db = Arc::new(Db::new());
        for i in 0..500 {
            let k = Bytes::copy_from_slice(format!("e:{i}").as_bytes());
            db.insert_value_from_load(k, make_expired_sv("v")).await;
        }
        assert_eq!(db.get_key_count(), 500);

        let mgr = TtlManager::new(vec![db.clone()]);
        for _ in 0..64 {
            mgr.purge_expired_keys_with_sampling().await;
            if db.get_key_count() == 0 {
                break;
            }
        }
        assert_eq!(db.get_key_count(), 0);
    }
}
