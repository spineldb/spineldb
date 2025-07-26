// src/core/replication/handler.rs

//! Handles an incoming connection from a replica that has sent a `PSYNC` command.
//!
//! This handler is spawned by the `ConnectionHandler` when it detects a `PSYNC` command,
//! effectively "handing off" the TCP stream. Its sole responsibility is to manage the
//! synchronization process for that single replica. It decides whether to perform a
//! full resynchronization (sending the entire dataset via an SPLDB snapshot) or a
//! partial resynchronization (sending only the missed commands from the replication backlog).
//! After synchronization, it streams live command updates.

use crate::core::Command;
use crate::core::commands::generic::script::ScriptSubcommand;
use crate::core::protocol::RespFrame;
use crate::core::replication::sync::InitialSyncer;
use crate::core::state::{ReplicaStateInfo, ReplicaSyncState, ServerState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, broadcast};
use tracing::{info, warn};

/// `ReplicaHandler` manages the synchronization and command streaming process
/// for a single connected replica. It is generic over the stream type `S` to
/// support both plain TCP and TLS connections.
pub struct ReplicaHandler<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    state: Arc<ServerState>,
    addr: SocketAddr,
    stream: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> ReplicaHandler<S> {
    /// Creates a new `ReplicaHandler` for a given stream.
    pub fn new(state: Arc<ServerState>, addr: SocketAddr, stream: S) -> Self {
        Self {
            state,
            addr,
            stream,
        }
    }

    /// The main entry point for the replica handler task.
    /// This function handles the entire lifecycle of the replica's session,
    /// including graceful shutdown and resource cleanup.
    pub async fn run(
        mut self,
        repl_id: String,
        offset_str: String,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        // Use tokio::select! to race the sync process against a shutdown signal.
        let sync_result = tokio::select! {
            biased; // Prioritize the shutdown signal.
            _ = shutdown_rx.recv() => {
                info!("Replica handler for {} received kill signal. Aborting.", self.addr);
                Err(anyhow::anyhow!("Killed by CLIENT KILL command"))
            }
            res = self.perform_sync_cycle(repl_id, offset_str) => {
                res
            }
        };

        if let Err(e) = sync_result {
            warn!("Replication sync cycle for {} ended: {}", self.addr, e);
        }

        // Cleanup: Ensure the replica's state is removed from the primary's global maps
        // when the connection is terminated for any reason.
        info!(
            "Replica handler for {} is terminating. Cleaning up its state.",
            self.addr
        );
        self.state.replica_states.remove(&self.addr);
        self.state.replica_sync_locks.remove(&self.addr);
    }

    /// Contains the core logic for a single synchronization attempt.
    async fn perform_sync_cycle(
        &mut self,
        repl_id: String,
        offset_str: String,
    ) -> Result<(), anyhow::Error> {
        // Use a lock to prevent multiple concurrent `PSYNC` attempts from the same replica IP:port.
        let sync_lock = self
            .state
            .replica_sync_locks
            .entry(self.addr)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let Ok(_guard) = sync_lock.try_lock() else {
            warn!(
                "Another sync process is already running for replica {}. Aborting this one.",
                self.addr
            );
            let _ = self.stream.write_all(b"-ERR Sync in progress\r\n").await;
            return Err(anyhow::anyhow!("Sync already in progress"));
        };

        info!(
            "Replica at {} requested sync with id '{}' and offset '{}'",
            self.addr, repl_id, offset_str
        );

        let master_replid = &self.state.replication.replication_info.master_replid;
        let replica_state = self
            .state
            .replica_states
            .get(&self.addr)
            .map(|r| r.value().sync_state);

        // --- Decision: Partial vs. Full Resync ---
        // A partial resync is possible if:
        // 1. The replica's run ID matches the primary's current run ID.
        // 2. The replica was previously online (not in the middle of a full sync).
        // 3. The requested offset is still present in the replication backlog.
        if repl_id.eq_ignore_ascii_case(master_replid)
            && replica_state == Some(ReplicaSyncState::Online)
        {
            if let Ok(offset) = offset_str.parse::<u64>() {
                if let Some(missed_frames) = self.state.replication_backlog.get_since(offset).await
                {
                    let frames_only: Vec<RespFrame> =
                        missed_frames.into_iter().map(|(_, frame)| frame).collect();

                    let current_offset = self.state.replication.get_replication_offset();
                    if self.do_partial_resync(&frames_only).await.is_ok() {
                        // After sending the backlog, transition to streaming live updates.
                        self.stream_live_updates(current_offset).await;
                    }
                    return Ok(());
                }
            }
        }

        // --- Full Resync Path ---
        // If any of the partial resync conditions fail, we must perform a full resync.
        self.state.replica_states.insert(
            self.addr,
            ReplicaStateInfo {
                sync_state: ReplicaSyncState::AwaitingFullSync,
                ack_offset: 0,
                last_ack_time: Instant::now(),
            },
        );

        info!("Performing full resync for replica {}", self.addr);
        let sync_start_offset = self.do_full_resync().await?;

        // Update the replica's state to Online *after* the full sync is complete.
        let should_stream = {
            if let Some(mut entry) = self.state.replica_states.get_mut(&self.addr) {
                entry.value_mut().sync_state = ReplicaSyncState::Online;
                info!(
                    "Replica {} is now Online after successful full resync.",
                    self.addr
                );
                true
            } else {
                warn!("Replica {} disconnected during full resync.", self.addr);
                false
            }
        };

        if should_stream {
            // Transition to streaming live updates.
            self.stream_live_updates(sync_start_offset).await;
        }

        Ok(())
    }

    /// Sends a `+CONTINUE` response followed by the backlog of commands.
    async fn do_partial_resync(&mut self, frames: &[RespFrame]) -> Result<(), anyhow::Error> {
        info!("Performing partial resync for replica {}", self.addr);
        self.stream.write_all(b"+CONTINUE\r\n").await?;
        for frame in frames {
            let encoded = frame.encode_to_vec()?;
            self.stream.write_all(&encoded).await?;
        }
        info!("Partial resync for replica {} complete.", self.addr);
        Ok(())
    }

    /// Sends a `+FULLRESYNC` response, the SPLDB snapshot file, and any cached scripts.
    async fn do_full_resync(&mut self) -> Result<u64, anyhow::Error> {
        let master_replid = &self.state.replication.replication_info.master_replid;
        let master_repl_offset = self.state.replication.get_replication_offset();

        // 1. Send the FULLRESYNC header.
        let full_resync_response = format!("+FULLRESYNC {master_replid} {master_repl_offset}\r\n");
        self.stream
            .write_all(full_resync_response.as_bytes())
            .await?;
        info!(
            "Sent FULLRESYNC response to replica {} with offset {}.",
            self.addr, master_repl_offset
        );

        // 2. Generate and send the SPLDB snapshot.
        let spldb_bytes = crate::core::persistence::spldb::save_to_bytes(&self.state.dbs).await?;
        info!(
            "Generated SPLDB snapshot ({} bytes) for replica {}.",
            spldb_bytes.len(),
            self.addr
        );
        let mut syncer = InitialSyncer::new(&mut self.stream);
        syncer.send_snapshot_file(&spldb_bytes).await?;
        info!("Finished sending SPLDB file to replica {}.", self.addr);

        // 3. Send all cached Lua scripts to make the replica's state consistent.
        let all_scripts = self.state.scripting.get_all_scripts();
        if !all_scripts.is_empty() {
            info!(
                "Sending {} cached scripts to replica {}.",
                all_scripts.len(),
                self.addr
            );
            for script_body in all_scripts.values() {
                let cmd = Command::Script(crate::core::commands::generic::Script {
                    subcommand: ScriptSubcommand::Load(script_body.clone()),
                });
                let frame: RespFrame = cmd.into();
                let encoded = frame.encode_to_vec()?;
                self.stream.write_all(&encoded).await?;
            }
            info!("Finished sending scripts to replica {}.", self.addr);
        }

        Ok(master_repl_offset)
    }

    /// Enters a loop to stream live commands to a now-synchronized replica.
    async fn stream_live_updates(&mut self, mut last_known_offset: u64) {
        info!(
            "Replica {} is now in sync and receiving live updates from offset {}.",
            self.addr, last_known_offset
        );

        // Subscribe to the primary's offset notifier.
        let mut offset_receiver = self.state.replication_offset_receiver.clone();

        loop {
            // Wait for the primary to signal that new commands have been processed.
            if offset_receiver.changed().await.is_err() {
                warn!(
                    "Replication offset channel closed. Shutting down replica handler for {}.",
                    self.addr
                );
                return;
            }

            let current_global_offset = *offset_receiver.borrow();
            if last_known_offset >= current_global_offset {
                continue; // Spurious wakeup, no new data.
            }

            // Fetch the commands from the backlog since our last known offset.
            if let Some(frames_with_offsets) = self
                .state
                .replication_backlog
                .get_since(last_known_offset)
                .await
            {
                if frames_with_offsets.is_empty() {
                    last_known_offset = current_global_offset;
                    continue;
                }

                // Send each frame and update our local offset tracker.
                for (frame_offset, frame) in frames_with_offsets {
                    match frame.encode_to_vec() {
                        Ok(encoded) => {
                            let frame_len = encoded.len() as u64;
                            if self.stream.write_all(&encoded).await.is_err() {
                                warn!(
                                    "Failed to send update to replica {}. Connection lost. Last successful offset: {}",
                                    self.addr, last_known_offset
                                );
                                return;
                            }
                            last_known_offset = frame_offset + frame_len;
                        }
                        Err(e) => {
                            warn!(
                                "Failed to encode frame for replication: {e}. Closing connection with replica {}",
                                self.addr
                            );
                            return;
                        }
                    }
                }
            } else {
                // If get_since returns None, we've fallen too far behind the backlog.
                warn!(
                    "Lost position in backlog for replica {}. Closing connection to force full resync.",
                    self.addr
                );
                return;
            }
        }
    }
}
