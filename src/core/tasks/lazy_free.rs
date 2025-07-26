// src/core/tasks/lazy_free.rs

use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::core::storage::data_types::{CacheBody, DataValue, StoredValue};

/// A task responsible for asynchronous value deallocation, triggered by `UNLINK`
/// or `DEL` on large items. It also handles deleting on-disk cache files.
pub struct LazyFreeManager {
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
                    // Asynchronously handle on-disk file deletions for HttpCache values.
                    for value in &mut values_to_free {
                        if let DataValue::HttpCache { variants, .. } = &mut value.data {
                            for variant in variants.values_mut() {
                                if let CacheBody::OnDisk { path, .. } = &variant.body {
                                    let path_clone = path.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = tokio::fs::remove_file(&path_clone).await {
                                            warn!("LazyFreeManager failed to delete on-disk cache file {:?}: {}", path_clone, e);
                                        } else {
                                            debug!("LazyFreeManager deleted on-disk cache file {:?}", path_clone);
                                        }
                                    });
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
                                         let path_clone = path.clone();
                                         tokio::spawn(async move {
                                             if let Err(e) = tokio::fs::remove_file(&path_clone).await {
                                                 warn!("LazyFreeManager (shutdown) failed to delete on-disk cache file {:?}: {}", path_clone, e);
                                             }
                                         });
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
