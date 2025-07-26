// src/core/tasks/cache_purger.rs

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::core::commands::Command;
use crate::core::commands::command_trait::CommandExt;
use crate::core::commands::scan::glob_match;
use crate::core::state::ServerState;

/// The interval for the lazy cache purger.
const CACHE_PURGE_INTERVAL: Duration = Duration::from_secs(1);

pub struct CachePurgerTask {
    state: Arc<ServerState>,
}

impl CachePurgerTask {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

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

    async fn perform_purge_cycle(&self) {
        let patterns_to_purge: Vec<Bytes> = self
            .state
            .cache
            .purge_patterns
            .iter()
            .map(|e| e.key().clone())
            .take(10)
            .collect();
        if patterns_to_purge.is_empty() {
            return;
        }

        let db = self.state.get_db(0).unwrap();
        let mut keys_to_delete = Vec::new();
        let mut cursor = 0;

        loop {
            let (next_cursor, keys) = db.scan_keys(cursor, 100).await;
            for key in keys {
                let key_str = String::from_utf8_lossy(&key);
                for pattern in &patterns_to_purge {
                    if glob_match(pattern, key_str.as_bytes()) {
                        keys_to_delete.push(key.clone());
                        break;
                    }
                }
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        if !keys_to_delete.is_empty() {
            let unlink_cmd = Command::Unlink(crate::core::commands::generic::Unlink {
                keys: keys_to_delete,
            });
            let mut unlink_ctx = crate::core::storage::db::ExecutionContext {
                state: self.state.clone(),
                locks: db.determine_locks_for_command(&unlink_cmd).await,
                db: &db,
                command: Some(unlink_cmd.clone()),
                session_id: 0,
                authenticated_user: None,
            };
            if let Err(e) = unlink_cmd.execute(&mut unlink_ctx).await {
                warn!("Cache purger failed to unlink keys: {}", e);
            }
        }

        for pattern in patterns_to_purge {
            self.state.cache.purge_patterns.remove(&pattern);
        }
    }
}
