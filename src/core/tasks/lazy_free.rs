// src/core/tasks/lazy_free.rs

use crate::core::state::ServerState;
use crate::core::storage::cache_types::ManifestState;
use crate::core::storage::data_types::{CacheBody, DataValue, StoredValue};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

/// A task responsible for asynchronous value deallocation, triggered by `UNLINK`
/// or `DEL` on large items. It also handles deleting on-disk cache files.
pub struct LazyFreeManager {
    /// A shared reference to the server state.
    pub state: Arc<ServerState>,
    /// Receives vectors of `StoredValue`s that have been removed from the
    /// keyspace and now need their memory to be reclaimed.
    pub rx: mpsc::Receiver<Vec<StoredValue>>,
}

impl LazyFreeManager {
    /// Runs the main loop for the lazy-free manager.
    pub async fn run(mut self, mut shutdown_rx: broadcast::Receiver<()>) {
        debug!("Lazy-free (UNLINK) manager task started.");
        loop {
            tokio::select! {
                Some(mut values_to_free) = self.rx.recv() => {
                    for value in &mut values_to_free {
                        if let DataValue::HttpCache { variants, .. } = &mut value.data {
                            for variant in variants.values_mut() {
                                if let CacheBody::OnDisk { path, .. } = &variant.body {
                                    if let Err(e) = self.state.cache.log_manifest(ManifestState::PendingDelete, path.clone()).await {
                                        warn!("LazyFreeManager failed to log pending deletion for {:?}: {}", path, e);
                                    } else {
                                        debug!("LazyFreeManager logged {:?} for deletion.", path);
                                    }
                                }
                            }
                        }
                    }
                    debug!("Lazy-freed {} values.", values_to_free.len());
                }
                _ = shutdown_rx.recv() => {
                    info!("Lazy-free manager shutting down.");
                    self.rx.close();
                    while let Some(values) = self.rx.recv().await {
                         debug!("Draining {} lazy-free values on shutdown.", values.len());
                         for mut value in values {
                             if let DataValue::HttpCache { variants, .. } = &mut value.data {
                                 for variant in variants.values_mut() {
                                     if let CacheBody::OnDisk { path, .. } = &variant.body {
                                         if let Err(e) = self.state.cache.log_manifest(ManifestState::PendingDelete, path.clone()).await {
                                             warn!("LazyFreeManager (shutdown) failed to log pending deletion for {:?}: {}", path, e);
                                         }
                                     }
                                 }
                             }
                         }
                         tokio::task::yield_now().await;
                    }
                    return;
                }
            }
        }
    }
}
