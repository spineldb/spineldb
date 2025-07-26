// src/core/tasks/cache_gc.rs

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::core::state::ServerState;
use crate::core::storage::cache_types::{ManifestEntry, ManifestState};

/// The interval for the on-disk cache garbage collector.
const ON_DISK_CACHE_GC_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour
/// Grace period before a PENDING file is considered for deletion.
const GC_PENDING_GRACE_PERIOD: Duration = Duration::from_secs(300); // 5 minutes

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
                    if let Err(e) = garbage_collect_from_manifest(&self.state).await {
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

/// The core logic for a single garbage collection cycle based on the manifest.
pub async fn garbage_collect_from_manifest(state: &Arc<ServerState>) -> anyhow::Result<()> {
    let manifest_path = get_manifest_path(state).await?;
    if !manifest_path.exists() {
        return Ok(());
    }

    // Lock the manifest writer to prevent concurrent writes during GC.
    let _writer_guard = state.cache.manifest_writer.lock().await;

    let manifest_file = TokioFile::open(&manifest_path).await?;
    let mut reader = BufReader::new(manifest_file);
    let mut line = String::new();

    let mut latest_entries: HashMap<PathBuf, ManifestEntry> = HashMap::new();

    // 1. Read the entire manifest to get the latest state for each file path.
    while reader.read_line(&mut line).await? > 0 {
        if let Ok(entry) = serde_json::from_str::<ManifestEntry>(&line) {
            latest_entries.insert(entry.path.clone(), entry);
        }
        line.clear();
    }

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut deleted_count = 0;
    let mut new_manifest_content = String::new();

    // 2. Process each path and decide its fate.
    for (path, entry) in latest_entries {
        let mut keep_entry = true;
        match entry.state {
            ManifestState::Pending => {
                // If a file is stuck in PENDING for too long, it's from a crashed write.
                if entry.timestamp + GC_PENDING_GRACE_PERIOD.as_secs() < now_secs {
                    if let Err(e) = tokio::fs::remove_file(&path).await {
                        warn!("GC failed to remove stale PENDING file {:?}: {}", path, e);
                    } else {
                        deleted_count += 1;
                    }
                    keep_entry = false; // Don't keep this stale entry in the new manifest.
                }
            }
            ManifestState::PendingDelete => {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    // It might have already been deleted, which is fine.
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!("GC failed to remove PENDING_DELETE file {:?}: {}", path, e);
                    } else {
                        deleted_count += 1;
                    }
                } else {
                    deleted_count += 1;
                }
                keep_entry = false; // This entry has been processed.
            }
            ManifestState::Committed => {
                // Keep committed entries in the manifest.
            }
        }
        if keep_entry {
            new_manifest_content.push_str(&serde_json::to_string(&entry)?);
            new_manifest_content.push('\n');
        }
    }

    // 3. Atomically rewrite the manifest with only the valid, active entries.
    let temp_manifest_path = manifest_path.with_extension("tmp.gc");
    tokio::fs::write(&temp_manifest_path, new_manifest_content).await?;
    tokio::fs::rename(&temp_manifest_path, &manifest_path).await?;

    if deleted_count > 0 {
        info!(
            "On-disk cache GC cycle complete. Removed {} files.",
            deleted_count
        );
    } else {
        debug!("On-disk cache GC cycle complete. No files to remove.");
    }

    Ok(())
}

async fn get_manifest_path(state: &Arc<ServerState>) -> anyhow::Result<PathBuf> {
    let cache_path_str = state.config.lock().await.cache.on_disk_path.clone();
    if cache_path_str.is_empty() {
        return Err(anyhow::anyhow!("On-disk cache path is not configured."));
    }
    let cache_path = std::path::Path::new(&cache_path_str);
    Ok(cache_path.join("spineldb-cache.manifest"))
}
