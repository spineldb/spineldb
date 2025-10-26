// src/core/tasks/cache_lock_cleaner.rs

use crate::core::state::ServerState;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, info};

/// The interval at which the stale lock cleaner task runs.
const CLEANER_INTERVAL: Duration = Duration::from_secs(600); // 10 minutes

/// A task that periodically cleans up stale lock entries from the `swr_locks` map
/// to prevent slow memory leaks over time.
pub struct CacheLockCleanerTask {
    state: Arc<ServerState>,
}

impl CacheLockCleanerTask {
    /// Creates a new `CacheLockCleanerTask`.
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// Runs the main loop for the lock cleaner task.
    /// It periodically wakes up and checks for SWR and manual lock entries
    /// that can be safely removed.
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
                    let manual_cleaned = self.clean_manual_locks();
                    if swr_cleaned > 0 || manual_cleaned > 0 {
                        debug!(
                            "Cache lock cleaner: removed {} stale SWR locks and {} expired manual locks.",
                            swr_cleaned, manual_cleaned
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
    /// This logic uses `Weak::strong_count`, which is a robust way to check
    /// if any `Arc` pointers to the lock still exist. If the strong count is 0,
    /// the weak pointer is dangling and can be safely removed.
    fn clean_swr_locks(&self) -> usize {
        let before_count = self.state.cache.swr_locks.len();
        if before_count == 0 {
            return 0;
        }

        self.state
            .cache
            .swr_locks
            .retain(|_key, weak_lock| weak_lock.strong_count() > 0);

        let after_count = self.state.cache.swr_locks.len();
        before_count - after_count
    }

    /// Removes expired manual locks created by the `CACHE.LOCK` command.
    fn clean_manual_locks(&self) -> usize {
        let before_count = self.state.cache.manual_locks.len();
        if before_count == 0 {
            return 0;
        }

        let now = std::time::Instant::now();
        self.state
            .cache
            .manual_locks
            .retain(|_key, expiry| *expiry > now);

        let after_count = self.state.cache.manual_locks.len();
        before_count - after_count
    }
}
