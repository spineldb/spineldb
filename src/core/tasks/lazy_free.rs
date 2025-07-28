// src/core/tasks/lazy_free.rs

//! Implements the background task for asynchronous value deallocation.

use crate::core::state::ServerState;
use crate::core::storage::cache_types::ManifestState;
use crate::core::storage::data_types::{CacheBody, DataValue, StoredValue};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

/// The type of item sent to the lazy-free channel: a tuple of the key and its value.
pub type LazyFreeItem = (Bytes, StoredValue);

/// A task responsible for asynchronous value deallocation, triggered by `UNLINK`
/// or `DEL` on large items. It also handles deleting on-disk cache files.
pub struct LazyFreeManager {
    /// A shared reference to the server state.
    pub state: Arc<ServerState>,
    /// Receives vectors of `LazyFreeItem`s that have been removed from the
    /// keyspace and now need their memory to be reclaimed or files deleted.
    pub rx: mpsc::Receiver<Vec<LazyFreeItem>>,
}

impl LazyFreeManager {
    /// Runs the main loop for the lazy-free manager.
    pub async fn run(mut self, mut shutdown_rx: broadcast::Receiver<()>) {
        debug!("Lazy-free (UNLINK) manager task started.");
        loop {
            tokio::select! {
                Some(items_to_free) = self.rx.recv() => {
                    self.process_items(items_to_free).await;
                }
                _ = shutdown_rx.recv() => {
                    info!("Lazy-free manager shutting down. Draining remaining items.");
                    self.rx.close();
                    while let Some(items) = self.rx.recv().await {
                         self.process_items(items).await;
                         // Yield to allow other shutdown tasks to proceed.
                         tokio::task::yield_now().await;
                    }
                    info!("Lazy-free manager finished draining.");
                    return;
                }
            }
        }
    }

    /// Processes a vector of items marked for lazy freeing.
    async fn process_items(&self, items_to_free: Vec<LazyFreeItem>) {
        let items_len = items_to_free.len();
        for (key, mut value) in items_to_free {
            // Check if the value is an HttpCache item with on-disk variants.
            if let DataValue::HttpCache { variants, .. } = &mut value.data {
                for variant in variants.values_mut() {
                    if let CacheBody::OnDisk { path, .. } = &variant.body {
                        // Log that the file is pending deletion. The GC task will handle the actual removal.
                        if let Err(e) = self
                            .state
                            .cache
                            .log_manifest(key.clone(), ManifestState::PendingDelete, path.clone())
                            .await
                        {
                            warn!(
                                "LazyFreeManager failed to log pending deletion for {:?}: {}",
                                path, e
                            );
                        } else {
                            debug!("LazyFreeManager logged {:?} for deletion.", path);
                        }
                    }
                }
            }
        }
        // The actual memory deallocation happens here when `items_to_free` is dropped.
        debug!("Lazy-freed {} values.", items_len);
    }
}
