// src/core/tasks/replica_quorum_validator.rs

//! A background task that monitors a primary's connectivity to its replicas.
//! If the primary loses contact with a quorum of replicas for a configured
//! duration, it will enter a read-only "fenced" state to prevent split-brain.

use crate::config::ReplicationConfig;
use crate::core::state::ServerState;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// The interval at which the validator task checks replica connectivity.
const VALIDATOR_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// The background task struct for the replica quorum validator.
pub struct ReplicaQuorumValidatorTask {
    state: Arc<ServerState>,
}

impl ReplicaQuorumValidatorTask {
    /// Creates a new ReplicaQuorumValidatorTask.
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// The main run loop for the validator task.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        let (is_enabled, timeout_secs) = {
            let config = self.state.config.lock().await;
            match &config.replication {
                ReplicationConfig::Primary(primary_config) => (
                    primary_config.fencing_on_replica_disconnect,
                    primary_config.replica_quorum_timeout_secs,
                ),
                _ => (false, 0),
            }
        };

        if !is_enabled {
            info!("Replica quorum fencing is disabled. Validator task will not run.");
            return;
        }

        info!(
            "Replica quorum validator task started. Timeout: {}s",
            timeout_secs
        );
        let mut interval = tokio::time::interval(VALIDATOR_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.check_quorum_and_fence(timeout_secs).await {
                        warn!("Error in replica quorum check cycle: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Replica quorum validator task shutting down.");
                    return;
                }
            }
        }
    }

    /// Performs the core logic of checking replica connectivity and fencing if needed.
    async fn check_quorum_and_fence(&self, timeout_secs: u64) -> Result<(), anyhow::Error> {
        // Double-check the role in case of dynamic reconfiguration (e.g., via failover).
        if !matches!(
            self.state.config.lock().await.replication,
            ReplicationConfig::Primary(_)
        ) {
            // If we are no longer a primary, ensure any fencing is removed.
            if self
                .state
                .is_read_only_due_to_quorum_loss
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                self.state
                    .set_quorum_loss_read_only(false, "Node is no longer a primary.");
            }
            return Ok(());
        }

        let total_replicas = self.state.replica_states.len();
        if total_replicas == 0 {
            // No replicas to check, nothing to do.
            return Ok(());
        }

        // The quorum is the majority of connected replicas.
        let quorum = (total_replicas / 2) + 1;
        let timeout_duration = Duration::from_secs(timeout_secs);

        let active_replicas = self
            .state
            .replica_states
            .iter()
            .filter(|entry| entry.value().last_ack_time.elapsed() <= timeout_duration)
            .count();

        let is_fenced = self
            .state
            .is_read_only_due_to_quorum_loss
            .load(std::sync::atomic::Ordering::Relaxed);

        if active_replicas < quorum {
            if !is_fenced {
                let reason = format!(
                    "Lost contact with replica quorum (see {active_replicas}/{total_replicas} replicas). Fencing master."
                );
                self.state.set_quorum_loss_read_only(true, &reason);
            }
        } else if is_fenced {
            // We have a quorum and we are fenced, so we should un-fence.
            // This checks that we are not fenced for another reason (like master quorum loss).
            let reason = format!(
                "Re-established contact with replica quorum (see {active_replicas}/{total_replicas} replicas). Un-fencing master."
            );
            self.state.set_quorum_loss_read_only(false, &reason);
        }

        Ok(())
    }
}
