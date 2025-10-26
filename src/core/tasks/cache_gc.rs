// src/core/tasks/cache_gc.rs

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::fs::{self, File as TokioFile, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::core::state::ServerState;
use crate::core::storage::cache_types::{ManifestEntry, ManifestState};

/// The interval for the on-disk cache garbage collector and compactor.
const GC_COMPACTION_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour
/// Grace period before a PENDING file is considered for deletion.
const GC_PENDING_GRACE_PERIOD: Duration = Duration::from_secs(300); // 5 minutes

/// A task that periodically cleans up orphaned cache files and compacts the manifest.
pub struct OnDiskCacheGCTask {
    state: Arc<ServerState>,
}

impl OnDiskCacheGCTask {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// The main run loop for the garbage collection and compaction task.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!("On-disk cache GC and compaction task started.");
        let mut interval = tokio::time::interval(GC_COMPACTION_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    info!("Running periodic on-disk cache GC and compaction cycle...");
                    if let Err(e) = garbage_collect_and_compact_manifest(&self.state).await {
                        warn!("On-disk cache GC/compaction cycle failed: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("On-disk cache GC and compaction task shutting down.");
                    return;
                }
            }
        }
    }
}

/// The core logic for a single garbage collection and compaction cycle.
pub async fn garbage_collect_and_compact_manifest(state: &Arc<ServerState>) -> anyhow::Result<()> {
    let manifest_path = get_manifest_path(state).await?;
    if !manifest_path.exists() {
        debug!("Manifest file does not exist, skipping GC/compaction cycle.");
        return Ok(());
    }

    // --- Phase 1: Lock, close, and read the existing manifest ---
    let latest_entries = {
        let mut writer_guard = state.cache.manifest_writer.lock().await;

        // Take ownership of the writer, closing the file handle. This is critical.
        if let Some(mut writer) = writer_guard.take() {
            writer.flush().await?;
        }
        drop(writer_guard); // Release the lock on the Option<>, not the file itself.

        let manifest_file = TokioFile::open(&manifest_path).await?;
        let mut reader = BufReader::new(manifest_file);
        let mut line = String::new();
        let mut entries: HashMap<PathBuf, ManifestEntry> = HashMap::new();

        // Read the entire manifest to get the latest state for each file path.
        while reader.read_line(&mut line).await? > 0 {
            if let Ok(entry) = serde_json::from_str::<ManifestEntry>(&line) {
                entries.insert(entry.path.clone(), entry);
            }
            line.clear();
        }
        entries
    };

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut deleted_files_count = 0;
    let mut new_manifest_content = String::new();

    // --- Phase 2: Process entries for GC and build the new, compacted manifest content ---
    for (_path, entry) in latest_entries {
        let mut keep_entry_in_new_manifest = false;

        match entry.state {
            ManifestState::Pending => {
                if entry.timestamp + GC_PENDING_GRACE_PERIOD.as_secs() < now_secs {
                    // This file was part of a write that likely crashed. Clean it up.
                    match fs::remove_file(&entry.path).await {
                        Ok(_) => deleted_files_count += 1,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // Already gone, still count it as "cleaned up".
                            deleted_files_count += 1;
                        }
                        Err(e) => {
                            warn!(
                                "GC failed to remove stale PENDING file {:?}: {}",
                                entry.path, e
                            );
                            keep_entry_in_new_manifest = true; // Retry next time
                        }
                    }
                } else {
                    // The write might still be in progress. Keep the entry for the next cycle.
                    keep_entry_in_new_manifest = true;
                }
            }
            ManifestState::PendingDelete => {
                // This file is marked for deletion.
                match fs::remove_file(&entry.path).await {
                    Ok(_) => deleted_files_count += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Already gone, successfully cleaned.
                        deleted_files_count += 1;
                    }
                    Err(e) => {
                        warn!(
                            "GC failed to remove PENDING_DELETE file {:?}: {}",
                            entry.path, e
                        );
                        keep_entry_in_new_manifest = true; // Retry next time
                    }
                }
            }
            ManifestState::Committed => {
                // Keep committed entries, but verify the file still exists on disk.
                if fs::metadata(&entry.path).await.is_ok() {
                    keep_entry_in_new_manifest = true;
                } else {
                    warn!(
                        "Manifest entry for {:?} is committed, but file not found. Discarding entry.",
                        entry.path
                    );
                }
            }
        }

        if keep_entry_in_new_manifest {
            new_manifest_content.push_str(&serde_json::to_string(&entry)?);
            new_manifest_content.push('\n');
        }
    }

    // --- Phase 3: Atomically rewrite the manifest and re-open the writer ---
    {
        let temp_manifest_path = manifest_path.with_extension("tmp.gc_compact");
        fs::write(&temp_manifest_path, &new_manifest_content).await?;
        fs::rename(&temp_manifest_path, &manifest_path).await?;

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&manifest_path)
            .await?;

        let new_writer = BufWriter::new(new_file);

        let mut writer_guard = state.cache.manifest_writer.lock().await;
        *writer_guard = Some(new_writer);
    }

    if deleted_files_count > 0 {
        info!(
            "On-disk cache GC/compaction cycle complete. Removed {} files.",
            deleted_files_count
        );
    } else {
        debug!("On-disk cache GC/compaction cycle complete. No files to remove.");
    }

    Ok(())
}

/// Helper to get the path to the cache manifest file.
async fn get_manifest_path(state: &Arc<ServerState>) -> anyhow::Result<PathBuf> {
    let cache_path_str = state.config.lock().await.cache.on_disk_path.clone();
    if cache_path_str.is_empty() {
        return Err(anyhow::anyhow!("On-disk cache path is not configured."));
    }
    let cache_path = std::path::Path::new(&cache_path_str);
    Ok(cache_path.join("spineldb-cache.manifest"))
}
