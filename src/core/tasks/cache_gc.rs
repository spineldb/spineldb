// src/core/tasks/cache_gc.rs

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::core::state::ServerState;
use crate::core::storage::data_types::{CacheBody, DataValue};

/// The interval for the on-disk cache garbage collector.
const ON_DISK_CACHE_GC_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour
/// Grace period before an orphaned file is considered for deletion.
/// This mitigates a race condition between file creation and in-memory state update.
const GC_GRACE_PERIOD: Duration = Duration::from_secs(300); // 5 minutes

/// A task that periodically cleans up orphaned cache files from the on-disk cache directory.
pub struct OnDiskCacheGCTask {
    state: Arc<ServerState>,
}

impl OnDiskCacheGCTask {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// The main run loop for the garbage collection task.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!("On-disk cache garbage collection task started.");
        let mut interval = tokio::time::interval(ON_DISK_CACHE_GC_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    info!("Running periodic on-disk cache garbage collection cycle...");
                    if let Err(e) = run_on_disk_cache_gc_cycle(&self.state).await {
                        warn!("On-disk cache GC cycle failed: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("On-disk cache garbage collection task shutting down.");
                    return;
                }
            }
        }
    }
}

/// The core logic for a single garbage collection cycle. This is now a shared function.
async fn run_on_disk_cache_gc_cycle(state: &Arc<ServerState>) -> anyhow::Result<()> {
    let cache_path_str = state.config.lock().await.cache.on_disk_path.clone();
    let cache_path = std::path::Path::new(&cache_path_str);

    if !cache_path.exists() {
        return Ok(());
    }

    let mut valid_paths = std::collections::HashSet::new();
    if let Some(db) = state.dbs.first() {
        for shard in &db.shards {
            let guard = shard.entries.lock().await;
            for value in guard.iter() {
                if let DataValue::HttpCache { variants, .. } = &value.1.data {
                    for variant in variants.values() {
                        if let CacheBody::OnDisk { path, .. } = &variant.body {
                            valid_paths.insert(path.clone());
                        }
                    }
                }
            }
        }
    }

    debug!(
        "Found {} valid on-disk cache entries in memory for GC.",
        valid_paths.len()
    );

    let mut orphaned_count = 0;
    let mut read_dir = tokio::fs::read_dir(cache_path).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            let is_tmp_file = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .ends_with(".tmp");

            if !valid_paths.contains(&path) || is_tmp_file {
                // Check the file's modification/creation time before deleting.
                if let Ok(metadata) = entry.metadata().await {
                    if let Ok(created_time) = metadata.created() {
                        if created_time < (SystemTime::now() - GC_GRACE_PERIOD) {
                            match tokio::fs::remove_file(&path).await {
                                Ok(_) => {
                                    debug!("Garbage collected orphaned cache file: {:?}", path);
                                    orphaned_count += 1;
                                }
                                Err(e) => {
                                    warn!("Failed to remove orphaned cache file {:?}: {}", path, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if orphaned_count > 0 {
        info!(
            "On-disk cache GC cycle complete. Removed {} orphaned files.",
            orphaned_count
        );
    } else {
        debug!("On-disk cache GC cycle complete. No orphaned files found.");
    }

    Ok(())
}

/// Scans the on-disk cache directory and removes any orphaned files at startup.
pub async fn garbage_collect_on_disk_cache(state: &Arc<ServerState>) -> anyhow::Result<()> {
    info!("Running startup garbage collection for on-disk cache...");
    run_on_disk_cache_gc_cycle(state).await
}
