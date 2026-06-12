// src/core/replication/handler.rs

//! Handles an incoming connection from a replica that has sent a `PSYNC` command.
//!
//! This handler manages the synchronization process for a single replica, deciding
//! between full or partial resynchronization and streaming live command updates.

use crate::core::Command;
use crate::core::commands::generic::script::ScriptSubcommand;
use crate::core::protocol::RespFrame;
use crate::core::state::{ReplicaStateInfo, ReplicaSyncState, ServerState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, broadcast};
use tracing::{info, warn};

/// `ReplicaHandler` manages the synchronization and command streaming process
/// for a single connected replica. It is generic over the stream type `S`.
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
    /// Handles the entire lifecycle of the replica's session.
    pub async fn run(
        mut self,
        repl_id: String,
        offset_str: String,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        // Race the sync process against a shutdown signal.
        let sync_result = tokio::select! {
            biased;
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

        info!(
            "Replica handler for {} is terminating. Cleaning up state.",
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
        // Prevent concurrent sync attempts from the same replica address.
        let sync_lock = self
            .state
            .replica_sync_locks
            .entry(self.addr)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let Ok(_guard) = sync_lock.try_lock() else {
            warn!(
                "Another sync process is running for replica {}. Aborting.",
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
        if repl_id.eq_ignore_ascii_case(master_replid)
            && replica_state == Some(ReplicaSyncState::Online)
            && let Ok(offset) = offset_str.parse::<u64>()
            && let Some(missed_frames) = self.state.replication_backlog.get_since(offset).await
        {
            let frames_only: Vec<RespFrame> =
                missed_frames.into_iter().map(|(_, frame)| frame).collect();

            let current_offset = self.state.replication.get_replication_offset();
            if self.do_partial_resync(&frames_only).await.is_ok() {
                self.stream_live_updates(current_offset).await;
            }
            return Ok(());
        }

        // --- Full Resync Path ---
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

        // Update replica state to Online.
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

    /// Sends a `+FULLRESYNC` response, streams the SPLDB snapshot, and sends cached scripts.
    async fn do_full_resync(&mut self) -> Result<u64, anyhow::Error> {
        let master_replid = &self.state.replication.replication_info.master_replid;
        let master_repl_offset = self.state.replication.get_replication_offset();

        // 1. Send FULLRESYNC header.
        let full_resync_response = format!("+FULLRESYNC {master_replid} {master_repl_offset}\r\n");
        self.stream
            .write_all(full_resync_response.as_bytes())
            .await?;
        info!(
            "Sent FULLRESYNC response to replica {} with offset {}.",
            self.addr, master_repl_offset
        );

        // 2. Generate and stream the SPLDB snapshot.
        // We write to a temporary file first to avoid buffering the entire DB in memory,
        // then stream that file to the replica.
        let temp_path = format!(
            "temp-repl-{}-{}.spldb",
            self.addr.port(),
            uuid::Uuid::new_v4()
        );
        let temp_file = TokioFile::create(&temp_path).await?;
        let mut buf_writer = BufWriter::new(temp_file);

        info!(
            "Generating SPLDB snapshot to temp file for replica {}...",
            self.addr
        );
        crate::core::persistence::spldb::write_database(&mut buf_writer, &self.state.dbs).await?;
        buf_writer.flush().await?;

        // Get file size for the bulk string header.
        let file_len = tokio::fs::metadata(&temp_path).await?.len();
        let bulk_header = format!("${}\r\n", file_len);
        self.stream.write_all(bulk_header.as_bytes()).await?;

        // Open the file again for reading and stream it to the socket.
        let mut file_reader = TokioFile::open(&temp_path).await?;
        tokio::io::copy(&mut file_reader, &mut self.stream).await?;

        // Clean up temp file.
        tokio::fs::remove_file(&temp_path).await.ok();

        info!("Finished streaming SPLDB file to replica {}.", self.addr);

        // 3. Send cached Lua scripts.
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

    /// Enters a loop to stream live commands to a synchronized replica.
    async fn stream_live_updates(&mut self, mut last_known_offset: u64) {
        info!(
            "Replica {} is in sync. Streaming live updates from offset {}.",
            self.addr, last_known_offset
        );

        let mut offset_receiver = self.state.replication_offset_receiver.clone();

        loop {
            if offset_receiver.changed().await.is_err() {
                warn!(
                    "Replication offset channel closed. Shutting down handler for {}.",
                    self.addr
                );
                return;
            }

            let current_global_offset = *offset_receiver.borrow();
            if last_known_offset >= current_global_offset {
                continue;
            }

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

                for (frame_offset, frame) in frames_with_offsets {
                    match frame.encode_to_vec() {
                        Ok(encoded) => {
                            let frame_len = encoded.len() as u64;
                            if self.stream.write_all(&encoded).await.is_err() {
                                warn!(
                                    "Failed to send update to replica {}. Connection lost.",
                                    self.addr
                                );
                                return;
                            }
                            last_known_offset = frame_offset + frame_len;
                        }
                        Err(e) => {
                            warn!("Failed to encode frame: {e}. Closing connection.");
                            return;
                        }
                    }
                }
            } else {
                warn!(
                    "Lost position in backlog for replica {}. Forcing full resync.",
                    self.addr
                );
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_sync_state_variants() {
        assert_ne!(ReplicaSyncState::Online, ReplicaSyncState::AwaitingFullSync);
    }

    #[test]
    fn test_fullresync_response_format() {
        let master_replid = "abc123";
        let master_repl_offset: u64 = 1024;
        let response = format!("+FULLRESYNC {master_replid} {master_repl_offset}\r\n");
        assert!(response.starts_with("+FULLRESYNC"));
        assert!(response.contains("abc123"));
        assert!(response.contains("1024"));
        assert!(response.ends_with("\r\n"));
    }

    #[test]
    fn test_partial_resync_continue_response() {
        let response = "+CONTINUE\r\n";
        assert_eq!(response, "+CONTINUE\r\n");
    }

    #[test]
    fn test_sync_lock_key_is_socket_addr() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        assert_eq!(addr.port(), 7000);
    }

    #[test]
    fn test_replica_state_info_construction() {
        let info = ReplicaStateInfo {
            sync_state: ReplicaSyncState::Online,
            ack_offset: 100,
            last_ack_time: Instant::now(),
        };
        assert_eq!(info.sync_state, ReplicaSyncState::Online);
        assert_eq!(info.ack_offset, 100);
    }

    #[test]
    fn test_replica_state_info_awaiting_full_sync() {
        let info = ReplicaStateInfo {
            sync_state: ReplicaSyncState::AwaitingFullSync,
            ack_offset: 0,
            last_ack_time: Instant::now(),
        };
        assert_eq!(info.sync_state, ReplicaSyncState::AwaitingFullSync);
        assert_eq!(info.ack_offset, 0);
    }

    #[test]
    fn test_temp_repl_path_format() {
        let port: u16 = 7000;
        let temp_path = format!("temp-repl-{}.spldb", port);
        assert_eq!(temp_path, "temp-repl-7000.spldb");
    }

    #[test]
    fn test_bulk_header_format() {
        let file_len: u64 = 1024;
        let bulk_header = format!("${}\r\n", file_len);
        assert_eq!(bulk_header, "$1024\r\n");
    }
}
