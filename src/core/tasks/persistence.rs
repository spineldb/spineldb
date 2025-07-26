// src/core/tasks/persistence.rs

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::info;

use crate::core::persistence::rewrite_aof;
use crate::core::state::ServerState;

/// The interval for the AOF rewrite manager to check conditions.
const AOF_REWRITE_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// A task that periodically checks if an AOF rewrite should be triggered
/// based on the file size growth.
pub struct AofRewriteManager {
    state: Arc<ServerState>,
}

impl AofRewriteManager {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// Runs the main loop for the AOF rewrite manager.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        let (enabled, min_size, percentage) = {
            let config = self.state.config.lock().await;
            (
                config.persistence.aof_enabled,
                config.persistence.auto_aof_rewrite_min_size,
                config.persistence.auto_aof_rewrite_percentage,
            )
        };

        if !enabled || percentage == 0 {
            info!("AOF auto-rewrite is disabled. Manager task will not run.");
            return;
        }

        if let Ok(metadata) =
            tokio::fs::metadata(&self.state.config.lock().await.persistence.aof_path).await
        {
            self.state
                .persistence
                .aof_last_rewrite_size
                .store(metadata.len(), Ordering::Relaxed);
        }

        info!(
            "AOF auto-rewrite manager started. Min size: {} bytes, Percentage: {}%",
            min_size, percentage
        );
        let mut interval = tokio::time::interval(AOF_REWRITE_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.check_and_trigger_rewrite(min_size, percentage).await;
                }
                _ = shutdown_rx.recv() => {
                    info!("AOF auto-rewrite manager shutting down.");
                    return;
                }
            }
        }
    }

    /// Checks the AOF size and triggers a rewrite if conditions are met.
    async fn check_and_trigger_rewrite(&self, min_size: u64, percentage: u64) {
        if self
            .state
            .persistence
            .is_saving_spldb
            .load(Ordering::Relaxed)
            || self
                .state
                .persistence
                .aof_rewrite_state
                .lock()
                .await
                .is_in_progress
        {
            return;
        }

        let aof_path = &self.state.config.lock().await.persistence.aof_path;
        let current_size = match tokio::fs::metadata(aof_path).await {
            Ok(metadata) => metadata.len(),
            Err(_) => return,
        };

        let last_size = self
            .state
            .persistence
            .aof_last_rewrite_size
            .load(Ordering::Relaxed);

        if last_size == 0 {
            if current_size > min_size {
                info!(
                    "AOF rewrite condition met (initial). Current size: {}, Min size: {}.",
                    current_size, min_size
                );
                self.trigger_rewrite().await;
            }
            return;
        }

        let growth_percentage = if current_size > last_size {
            ((current_size - last_size) * 100) / last_size
        } else {
            0
        };

        if current_size > min_size && growth_percentage >= percentage {
            info!(
                "AOF rewrite condition met. Current size: {}, Last size: {}, Growth: {}% (Threshold: {}%)",
                current_size, last_size, growth_percentage, percentage
            );
            self.trigger_rewrite().await;
        }
    }

    /// Spawns the `rewrite_aof` function in a new task.
    async fn trigger_rewrite(&self) {
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            rewrite_aof(state_clone).await;
        });
    }
}
