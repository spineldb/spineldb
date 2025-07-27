// src/core/replication/mod.rs

//! This module orchestrates the replication subsystem, setting up the appropriate
//! role (primary or replica) based on the server's configuration.

use crate::config::ReplicationConfig;
use crate::core::state::ServerState;
use crate::core::{Command, SpinelDBError};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::broadcast;
use tokio::task::JoinError;
use tracing::{info, warn};

// Public sub-modules for replication.
pub mod backlog;
pub mod handler;
pub mod sync;
pub mod worker;

/// Sets up the appropriate replication task based on the server's configuration.
///
/// Returns a `JoinHandle` to the spawned task, allowing the main server loop to
/// monitor its health.
pub async fn setup_replication(
    state: Arc<ServerState>,
    shutdown_rx: broadcast::Receiver<()>,
    reconfigure_rx: broadcast::Receiver<()>,
) -> Result<impl Future<Output = Result<(), JoinError>>, SpinelDBError> {
    let replication_role = state.config.lock().await.replication.clone();

    match replication_role {
        // If the server is a primary, start the backlog feeder task.
        ReplicationConfig::Primary(_) => {
            info!("Server starting in PRIMARY mode. Spawning replication backlog feeder.");
            let handle = tokio::spawn(run_backlog_feeder(state, shutdown_rx));
            Ok(handle)
        }
        // If the server is a replica, start the replication worker task.
        ReplicationConfig::Replica { .. } => {
            info!("Server starting in REPLICA mode. Spawning replication worker.");
            let worker = worker::ReplicaWorker::new(state);
            let handle = tokio::spawn(worker.run(shutdown_rx, reconfigure_rx));
            Ok(handle)
        }
    }
}

/// A background task for a primary server that listens to the event bus and feeds
/// write commands into the replication backlog.
async fn run_backlog_feeder(state: Arc<ServerState>, mut shutdown_rx: broadcast::Receiver<()>) {
    let mut event_rx = state.event_bus.subscribe_for_replication();
    info!("Replication backlog feeder task is running.");

    loop {
        tokio::select! {
            result = event_rx.recv() => {
                match result {
                    Ok(work) => {
                        // The `UnitOfWork` received from the event bus has already had any
                        // necessary transformations (like EVALSHA -> EVAL) applied by the router
                        // or transaction handler, ensuring it's safe for propagation.
                        let commands_to_propagate = match work.uow {
                            crate::core::events::UnitOfWork::Command(cmd) => {
                                vec![*cmd]
                            },
                            crate::core::events::UnitOfWork::Transaction(tx_data) => {
                                // For replication, only propagate commands that actually modify data.
                                if tx_data.write_commands.is_empty() {
                                    continue;
                                }
                                // Wrap the write commands in MULTI/EXEC for atomic execution on replicas.
                                let mut full_tx = Vec::with_capacity(tx_data.write_commands.len() + 2);
                                full_tx.push(Command::Multi);
                                full_tx.extend(tx_data.write_commands);
                                full_tx.push(Command::Exec);
                                full_tx
                            }
                        };

                        for cmd in commands_to_propagate {
                            let frame: crate::core::protocol::RespFrame = cmd.into();
                            if let Ok(encoded) = frame.encode_to_vec() {
                                let frame_len = encoded.len() as u64;
                                // Atomically get the current offset and add the frame length to it.
                                let command_offset = state
                                    .replication.replication_info
                                    .master_repl_offset
                                    .fetch_add(frame_len, Ordering::SeqCst);
                                // Add the command and its offset to the backlog.
                                state.replication_backlog.add(command_offset, frame, frame_len as usize).await;
                            }
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Replication backlog feeder lagged. {} events were dropped. This may cause replicas to require a full resync.", n);
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Event bus channel closed. Replication backlog feeder shutting down.");
                        break;
                    }
                }
            },
            _ = shutdown_rx.recv() => {
                info!("Replication backlog feeder shutting down.");
                return;
            }
        }
    }
}
