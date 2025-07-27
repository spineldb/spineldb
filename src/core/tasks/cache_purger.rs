// src/core/tasks/cache_purger.rs

//! A background task that performs lazy, pattern-based cache purging.
//!
//! This task periodically scans the keyspace and deletes keys that match
//! patterns submitted via the `CACHE.PURGE` command. It operates incrementally
//! to avoid blocking the server for extended periods.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{info, warn};
use wildmatch::WildMatch;

use crate::core::commands::Command;
use crate::core::commands::command_trait::CommandExt;
use crate::core::state::ServerState;

/// The interval at which the lazy cache purger runs its cycle.
const CACHE_PURGE_INTERVAL: Duration = Duration::from_secs(1);

/// The background task struct for the lazy cache purger.
pub struct CachePurgerTask {
    state: Arc<ServerState>,
}

impl CachePurgerTask {
    /// Creates a new `CachePurgerTask`.
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// The main run loop for the purger task.
    /// It periodically calls `perform_purge_cycle` to check for and delete matched keys.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!("Cache purger task started.");
        let mut interval = tokio::time::interval(CACHE_PURGE_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.perform_purge_cycle().await;
                }
                _ = shutdown_rx.recv() => {
                    info!("Cache purger task shutting down.");
                    return;
                }
            }
        }
    }

    /// Performs a single purge cycle.
    ///
    /// It takes a batch of patterns from the purge queue, scans the entire keyspace
    /// incrementally, and collects any keys that match one of the patterns. Finally,
    /// it deletes the matched keys using `UNLINK`.
    async fn perform_purge_cycle(&self) {
        // Take a small batch of purge patterns to process in this cycle.
        let patterns_to_purge: Vec<Bytes> = self
            .state
            .cache
            .purge_patterns
            .iter()
            .map(|e| e.key().clone())
            .take(10) // Process up to 10 patterns per cycle to avoid excessive work.
            .collect();

        if patterns_to_purge.is_empty() {
            return;
        }

        // Pre-compile glob patterns for efficiency within the scan loop.
        let matchers: Vec<_> = patterns_to_purge
            .iter()
            .map(|p| WildMatch::new(&String::from_utf8_lossy(p)))
            .collect();

        let db = self.state.get_db(0).unwrap(); // Assumes cache is on DB 0.
        let mut keys_to_delete = Vec::new();
        let mut cursor = 0;

        loop {
            // Scan the keyspace incrementally to avoid blocking the server.
            let (next_cursor, keys) = db.scan_keys(cursor, 100).await;

            for key in keys {
                let key_str = String::from_utf8_lossy(&key);
                // Check each key against the batch of patterns.
                for matcher in &matchers {
                    if matcher.matches(&key_str) {
                        keys_to_delete.push(key.clone());
                        break; // Move to the next key once a match is found.
                    }
                }
            }

            if next_cursor == 0 {
                break; // The entire keyspace has been scanned.
            }
            cursor = next_cursor;
        }

        if !keys_to_delete.is_empty() {
            // Asynchronously delete all matched keys using UNLINK for performance.
            let unlink_cmd = Command::Unlink(crate::core::commands::generic::Unlink {
                keys: keys_to_delete,
            });
            let mut unlink_ctx = crate::core::storage::db::ExecutionContext {
                state: self.state.clone(),
                locks: db.determine_locks_for_command(&unlink_cmd).await,
                db: &db,
                command: Some(unlink_cmd.clone()),
                session_id: 0, // Internal operation, no session ID.
                authenticated_user: None,
            };
            if let Err(e) = unlink_cmd.execute(&mut unlink_ctx).await {
                warn!("Cache purger failed to unlink keys: {}", e);
            }
        }

        // Remove the processed patterns from the queue.
        for pattern in patterns_to_purge {
            self.state.cache.purge_patterns.remove(&pattern);
        }
    }
}
