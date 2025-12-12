// src/core/persistence/spldb_saver.rs

//! Implements the SPLDB auto-saver background task.
//! This task periodically checks if the configured `save` conditions (e.g., "save after
//! 900 seconds if at least 1 key changed") are met and triggers a background SPLDB save.

use crate::core::persistence::spldb;
use crate::core::state::ServerState;
use anyhow::{Result, anyhow};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::fs::File as TokioFile;
use tokio::io::BufWriter;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

/// The interval at which the saver task checks if save conditions are met.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// The background task struct for the SPLDB auto-saver.
pub struct SpldbSaverTask {
    state: Arc<ServerState>,
}

impl SpldbSaverTask {
    /// Creates a new SpldbSaverTask.
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// The main run loop for the SPLDB auto-saver.
    /// It periodically checks the save conditions and also handles graceful shutdown,
    /// performing a final save if necessary.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        let save_rules = {
            let config = self.state.config.lock().await;
            config.persistence.save_rules.clone()
        };

        if save_rules.is_empty() {
            info!("No 'save' rules configured. SPLDB auto-saver will not run.");
            return;
        }

        info!("SPLDB auto-saver task started.");
        let mut interval = tokio::time::interval(CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.should_save(&save_rules).await {
                        self.trigger_background_save();
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("SPLDB auto-saver task received shutdown signal.");
                    // Before shutting down, wait for any in-progress save to finish.
                    while self.state.persistence.is_saving_spldb.load(Ordering::SeqCst) {
                        debug!("Waiting for in-progress SPLDB save to finish before shutting down...");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    // Perform a final save if there are unsaved changes.
                    if self.state.persistence.dirty_keys_counter.load(Ordering::Relaxed) > 0 {
                        info!("Performing final SPLDB save on shutdown...");
                        if let Err(e) = Self::perform_save_logic(&self.state).await {
                           error!("Final SPLDB save on shutdown failed: {}", e);
                        }
                    }
                    info!("SPLDB auto-saver task finished.");
                    return;
                }
            }
        }
    }

    /// Triggers a background SPLDB save by spawning a new task.
    /// It uses an atomic flag (`is_saving_spldb`) to ensure only one save
    /// process runs at a time.
    fn trigger_background_save(&self) {
        // Use `compare_exchange` to atomically check and set the flag.
        if self
            .state
            .persistence
            .is_saving_spldb
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            debug!("SPLDB save already in progress. Skipping this trigger.");
            return;
        }
        info!("SPLDB save conditions met. Spawning background save task.");
        let state_clone = self.state.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::perform_save_logic(&state_clone).await {
                error!("Background SPLDB save failed: {}", e);
            }
            // Reset the flag once the save is complete (or has failed).
            state_clone
                .persistence
                .is_saving_spldb
                .store(false, Ordering::SeqCst);
        });
    }

    /// The core logic for performing an SPLDB save.
    /// This can be called by the auto-saver or directly for `BGSAVE` and `SAVE`.
    pub(crate) async fn perform_save_logic(state: &Arc<ServerState>) -> Result<()> {
        let start_time = Instant::now();
        let add_latency_sample = |state: &Arc<ServerState>| {
            let latency = start_time.elapsed();
            state
                .latency_monitor
                .add_sample("spldb-save", vec![], latency);
        };

        let dirty_at_start = state.persistence.dirty_keys_counter.load(Ordering::Relaxed);
        if dirty_at_start == 0 {
            info!("SPLDB save triggered, but no dirty keys found. Skipping.");
            add_latency_sample(state);
            return Ok(());
        }

        let config = state.config.lock().await;
        let path_clone = config.persistence.spldb_path.clone();
        drop(config);

        let temp_path_str = format!("{}.tmp.{}", path_clone, rand::random::<u32>());
        let temp_path = std::path::Path::new(&temp_path_str);

        // Step 1: Create file and stream the database state to it.
        // We open the file and wrap it in a BufWriter for efficiency.
        let file_result = TokioFile::create(&temp_path).await;
        match file_result {
            Ok(file) => {
                let mut writer = BufWriter::new(file);
                if let Err(e) = spldb::write_database(&mut writer, &state.dbs).await {
                    let err_msg = format!("Failed to write SPLDB snapshot to temporary file: {e}");
                    error!("{}", err_msg);
                    *state.persistence.last_save_failure_time.lock().await =
                        Some(std::time::Instant::now());

                    // Attempt to clean up the incomplete file.
                    if let Err(remove_err) = fs::remove_file(temp_path) {
                        error!(
                            "Additionally failed to remove temporary SPLDB file '{}': {remove_err}",
                            temp_path_str
                        );
                    }
                    add_latency_sample(state);
                    return Err(anyhow!(err_msg));
                }
            }
            Err(e) => {
                let err_msg = format!("Failed to create temporary SPLDB file: {e}");
                error!("{}", err_msg);
                return Err(anyhow!(err_msg));
            }
        }

        info!(
            "SPLDB snapshot saved successfully to temporary file {}",
            temp_path_str
        );

        // Step 2: Atomically rename the temporary file to the final destination.
        if let Err(e) = fs::rename(temp_path, &path_clone) {
            let reason = format!(
                "CRITICAL: Failed to rename temporary SPLDB file '{temp_path_str}' to '{path_clone}': {e}"
            );
            error!(
                "{}. The SPLDB file on disk is outdated. Entering read-only mode.",
                reason
            );

            // Set server to read-only on critical rename failure.
            state.set_read_only(true, &reason);
            *state.persistence.last_save_failure_time.lock().await =
                Some(std::time::Instant::now());

            if let Err(remove_err) = fs::remove_file(temp_path) {
                error!(
                    "Additionally failed to remove temporary SPLDB file '{}': {remove_err}",
                    temp_path_str
                );
            }
            add_latency_sample(state);
            return Err(anyhow!(reason));
        }

        // Step 3: Success.
        info!("SPLDB file successfully saved to {}", path_clone);
        // Atomically subtract the number of keys that were dirty when we started.
        // This is safe because `fetch_sub` handles concurrent additions correctly.
        state
            .persistence
            .dirty_keys_counter
            .fetch_sub(dirty_at_start, Ordering::Relaxed);
        *state.persistence.last_save_success_time.lock().await = Some(std::time::Instant::now());
        add_latency_sample(state);
        Ok(())
    }

    /// Checks if any of the configured `save` rules are met.
    async fn should_save(&self, save_rules: &[crate::config::SaveRule]) -> bool {
        let dirty_keys = self
            .state
            .persistence
            .dirty_keys_counter
            .load(Ordering::Relaxed);
        if dirty_keys == 0 {
            return false;
        }
        let elapsed_since_last_save = {
            let last_success_time_guard =
                self.state.persistence.last_save_success_time.lock().await;
            if let Some(last_success_time) = *last_success_time_guard {
                last_success_time.elapsed()
            } else {
                // If no successful save yet, consider it a very long time ago to trigger the first save.
                Duration::from_secs(u64::MAX)
            }
        };
        for rule in save_rules {
            if elapsed_since_last_save.as_secs() >= rule.seconds && dirty_keys >= rule.changes {
                info!(
                    "SPLDB save condition met: {} changes in {} seconds (rule: save {} {}).",
                    dirty_keys,
                    elapsed_since_last_save.as_secs(),
                    rule.seconds,
                    rule.changes
                );
                return true;
            }
        }
        false
    }
}
