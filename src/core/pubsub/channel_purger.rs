// src/core/pubsub/channel_purger.rs

//! A background task to periodically clean up empty Pub/Sub channels.

use crate::core::state::ServerState;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::info;

/// The interval at which the purger task runs. 5 minutes is a reasonable default.
const PURGE_INTERVAL: Duration = Duration::from_secs(300);

/// The background task struct for the channel purger.
pub struct ChannelPurgerTask {
    state: Arc<ServerState>,
}

impl ChannelPurgerTask {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// The main run loop for the purger task.
    /// It periodically calls the `purge_empty_channels` method on the `PubSubManager`.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        info!("Pub/Sub channel purger task started.");
        let mut interval = tokio::time::interval(PURGE_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.state.pubsub.purge_empty_channels();
                }
                _ = shutdown_rx.recv() => {
                    info!("Pub/Sub channel purger task shutting down.");
                    return;
                }
            }
        }
    }
}
