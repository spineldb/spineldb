// src/core/tasks/on_disk_cache_eviction.rs

//! A background task to enforce the `max_disk_size` limit for the on-disk cache.

use crate::core::state::ServerState;
use crate::core::storage::cache_types::{ManifestEntry, ManifestState};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;
use tracing::{info, warn};

/// The interval at which the on-disk eviction task runs.
const EVICTION_INTERVAL: Duration = Duration::from_secs(10);

/// A task responsible for proactive on-disk cache eviction.
pub struct OnDiskCacheEvictionTask {
    state: Arc<ServerState>,
}

impl OnDiskCacheEvictionTask {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// The main run loop for the eviction task.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        let max_disk_size = self.state.config.lock().await.cache.max_disk_size;
        if max_disk_size == 0 {
            info!("On-disk cache eviction is disabled (max_disk_size = 0). Task will not run.");
            return;
        }
        info!("On-disk cache eviction task started.");
        let mut interval = tokio::time::interval(EVICTION_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.perform_eviction_cycle(max_disk_size).await {
                        warn!("On-disk cache eviction cycle failed: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("On-disk cache eviction task shutting down.");
                    return;
                }
            }
        }
    }

    /// Performs a single eviction cycle if the cache size exceeds the limit.
    async fn perform_eviction_cycle(&self, max_disk_size: u64) -> Result<()> {
        let manifest_path = get_manifest_path(&self.state).await?;
        if !manifest_path.exists() {
            return Ok(());
        }

        // Lock the manifest writer to prevent concurrent writes during eviction checks.
        let _writer_guard = self.state.cache.manifest_writer.lock().await;

        let manifest_file = TokioFile::open(&manifest_path).await?;
        let mut reader = BufReader::new(manifest_file);
        let mut line = String::new();

        let mut committed_entries: Vec<ManifestEntry> = Vec::new();
        let mut total_size = 0;

        // Read the manifest to get a list of all committed files and their total size.
        while reader.read_line(&mut line).await? > 0 {
            if let Ok(entry) = serde_json::from_str::<ManifestEntry>(&line) {
                if entry.state == ManifestState::Committed {
                    if let Ok(metadata) = tokio::fs::metadata(&entry.path).await {
                        total_size += metadata.len();
                        committed_entries.push(entry);
                    }
                }
            }
            line.clear();
        }

        if total_size <= max_disk_size {
            return Ok(()); // Quota not exceeded.
        }

        info!(
            "On-disk cache size ({}) exceeds limit ({}). Starting eviction.",
            total_size, max_disk_size
        );

        // Sort by timestamp to find the least recently used files.
        committed_entries.sort_by_key(|e| e.timestamp);

        let mut size_to_free = total_size - max_disk_size;
        let mut evicted_count = 0;

        // Evict files until the size is under the quota.
        for entry in committed_entries {
            if size_to_free == 0 {
                break;
            }

            let file_size = match tokio::fs::metadata(&entry.path).await {
                Ok(m) => m.len(),
                Err(_) => continue,
            };

            // Log the file for deletion. The GC task will perform the actual file removal.
            self.state
                .cache
                .log_manifest(entry.key, ManifestState::PendingDelete, entry.path)
                .await?;

            size_to_free = size_to_free.saturating_sub(file_size);
            evicted_count += 1;
        }

        info!(
            "Evicted {} on-disk cache files to meet size limit.",
            evicted_count
        );
        Ok(())
    }
}

/// Helper to get the path to the cache manifest file.
async fn get_manifest_path(state: &Arc<ServerState>) -> Result<PathBuf> {
    let cache_path_str = state.config.lock().await.cache.on_disk_path.clone();
    let cache_path = std::path::Path::new(&cache_path_str);
    Ok(cache_path.join("spineldb-cache.manifest"))
}
