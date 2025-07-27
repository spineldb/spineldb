// src/core/tasks/cache_lock_cleaner.rs

use crate::core::state::ServerState;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, info};

/// The interval at which the stale lock cleaner task runs.
const CLEANER_INTERVAL: Duration = Duration::from_secs(600); // 10 minutes

/// A task that periodically cleans up stale lock entries from cache state maps
/// (`swr_locks`, `fetch_locks`) to prevent slow memory leaks over time.
pub struct CacheLockCleanerTask {
    state: Arc<ServerState>,
}

impl CacheLockCleanerTask {
    /// Creates a new `CacheLockCleanerTask`.
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// Runs the main loop for the lock cleaner task.
    /// It periodically wakes up and checks for lock entries that can be safely removed.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!(
            "Cache lock cleaner task started. Check interval: {:?}",
            CLEANER_INTERVAL
        );
        let mut interval = tokio::time::interval(CLEANER_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let swr_cleaned = self.clean_swr_locks();
                    let fetch_cleaned = self.clean_fetch_locks();
                    if swr_cleaned > 0 || fetch_cleaned > 0 {
                        debug!(
                            "Cache lock cleaner: removed {} stale SWR locks and {} stale fetch locks.",
                            swr_cleaned, fetch_cleaned
                        );
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Cache lock cleaner task shutting down.");
                    return;
                }
            }
        }
    }

    /// Removes SWR lock entries that are no longer being waited on.
    /// The logic relies on checking the `Arc` strong count. A strong count of 1
    /// indicates that only the `DashMap` itself holds a reference to the lock,
    /// meaning no task is currently holding or waiting for it.
    fn clean_swr_locks(&self) -> usize {
        let before_count = self.state.cache.swr_locks.len();
        if before_count == 0 {
            return 0;
        }

        self.state
            .cache
            .swr_locks
            .retain(|_key, lock_arc| Arc::strong_count(lock_arc) > 1);

        let after_count = self.state.cache.swr_locks.len();
        before_count - after_count
    }

    /// Removes FETCH lock entries that are no longer being waited on, using the same
    /// `Arc` strong count logic as `clean_swr_locks`. This is necessary now that
    /// the `remove()` call in `CACHE.FETCH` has been removed to fix a race condition.
    fn clean_fetch_locks(&self) -> usize {
        let before_count = self.state.cache.fetch_locks.len();
        if before_count == 0 {
            return 0;
        }

        self.state
            .cache
            .fetch_locks
            .retain(|_key, lock_arc| Arc::strong_count(lock_arc) > 1);

        let after_count = self.state.cache.fetch_locks.len();
        before_count - after_count
    }
}
