// src/core/replication/worker.rs

//! Implements the replication logic for a replica server.
//!
//! This worker is responsible for the entire lifecycle of a replica's connection
//! to its primary. It connects to the primary, performs a multi-step handshake,
//! handles both full (SPLDB snapshot) and partial (backlog) resynchronization, and
//! then enters a loop to process the continuous stream of write commands.
//! It is designed to be resilient, with an exponential backoff reconnection strategy,
//! and can be dynamically reconfigured to follow a new primary after a failover.

use crate::config::ReplicationConfig;
use crate::core::commands::command_trait::{CommandExt, CommandFlags};
use crate::core::commands::generic::Select;
use crate::core::persistence::spldb::load_from_bytes;
use crate::core::protocol::{RespFrame, RespFrameCodec};
use crate::core::state::{ReplicaInfo, ServerState};
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{Command, SpinelDBError};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use rand::Rng;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
    BufReader as TokioBufReader, ReadHalf, WriteHalf, split,
};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, broadcast};
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::{TlsConnector, rustls};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, warn};

// The initial delay before the first reconnection attempt.
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
// The maximum delay for the exponential backoff reconnection strategy.
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

// An enum to abstract over different stream types (plain TCP or TLS),
// allowing the replication logic to be generic over the transport layer.
enum ReplicaStream {
    Tcp(TcpStream),
    Tls(Box<ClientTlsStream<TcpStream>>),
}

// --- Trait Implementations for ReplicaStream ---
// These implementations simply delegate the calls to the underlying stream type.

impl AsyncRead for ReplicaStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ReplicaStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            ReplicaStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ReplicaStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            ReplicaStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            ReplicaStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            ReplicaStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            ReplicaStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            ReplicaStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            ReplicaStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// The result of a successful handshake with the primary.
#[derive(PartialEq, Debug)]
enum HandshakeResult {
    /// The primary requires a full resynchronization (SPLDB transfer).
    FullResync,
    /// The primary will send only the missed commands from its backlog.
    PartialResync,
}

/// The main worker task for a replica server.
pub struct ReplicaWorker {
    state: Arc<ServerState>,
    /// The currently selected database index, received from the primary's command stream.
    current_db_index: usize,
    /// A flag indicating if the replica is currently processing a `MULTI`/`EXEC` block.
    is_in_transaction: bool,
    /// A queue for commands received between `MULTI` and `EXEC`.
    queued_tx_commands: Vec<Command>,
    /// Tracks the last known primary to detect configuration changes from failovers.
    last_known_primary: Mutex<Option<(String, u16)>>,
}

impl ReplicaWorker {
    /// Creates a new `ReplicaWorker`.
    pub fn new(state: Arc<ServerState>) -> Self {
        Self {
            state,
            current_db_index: 0,
            is_in_transaction: false,
            queued_tx_commands: Vec::new(),
            last_known_primary: Mutex::new(None),
        }
    }

    /// The main run loop for the replica worker. This loop manages the connection
    /// state and handles reconnection with exponential backoff.
    pub async fn run(
        mut self,
        mut shutdown_rx: broadcast::Receiver<()>,
        mut reconfigure_rx: broadcast::Receiver<()>,
    ) {
        info!("Replica worker started.");
        let mut current_delay = INITIAL_RECONNECT_DELAY;

        loop {
            // Proactively check the current replication configuration at the start of each cycle.
            // This makes the worker self-correcting if a reconfiguration signal is missed.
            let (current_primary_host, current_primary_port) = {
                let config_guard = self.state.config.lock().await;
                match &config_guard.replication {
                    ReplicationConfig::Replica {
                        primary_host,
                        primary_port,
                        ..
                    } => (primary_host.clone(), *primary_port),
                    _ => {
                        info!(
                            "Server role is no longer REPLICA. Shutting down replication worker."
                        );
                        return;
                    }
                }
            };

            let mut last_known = self.last_known_primary.lock().await;
            if last_known.as_ref() != Some(&(current_primary_host.clone(), current_primary_port)) {
                info!(
                    "Replication target changed to {}:{}. Resetting reconnect delay.",
                    current_primary_host, current_primary_port
                );
                *last_known = Some((current_primary_host, current_primary_port));
                current_delay = INITIAL_RECONNECT_DELAY; // Immediately try to connect to the new primary.
            }
            drop(last_known);

            tokio::select! {
                _ = reconfigure_rx.recv() => {
                    info!("Received replication reconfigure signal. Restarting connection cycle immediately.");
                    current_delay = INITIAL_RECONNECT_DELAY;
                    continue; // Re-enter the loop to check the new config and start a new connection cycle.
                }
                result = self.handle_connection_cycle() => {
                    if let Err(e) = result {
                        warn!("Replication cycle failed: {e}. Reconnecting...");
                    } else {
                        info!("Connection to primary closed cleanly. Reconnecting...");
                        current_delay = INITIAL_RECONNECT_DELAY; // Reset delay on clean disconnect.
                    }

                    // Apply exponential backoff with jitter to avoid thundering herd on primary restart.
                    let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..500));
                    let wait_time = current_delay + jitter;
                    info!("Will try to reconnect to primary in {wait_time:?}");

                    // Wait for the backoff period, but allow shutdown/reconfigure signals to interrupt.
                    tokio::select! {
                        _ = tokio::time::sleep(wait_time) => {}
                        _ = shutdown_rx.recv() => { info!("Replica worker shutting down during backoff."); return; }
                        _ = reconfigure_rx.recv() => { info!("Reconfigure signal received during backoff. Reconnecting immediately."); }
                    }

                    // Increase the delay for the next attempt.
                    current_delay = (current_delay * 2).min(MAX_RECONNECT_DELAY);
                }
                _ = shutdown_rx.recv() => {
                    info!("Replica worker shutting down.");
                    return;
                }
            }
        }
    }

    /// Manages a single connection lifecycle: connect, handshake, sync, and process command stream.
    async fn handle_connection_cycle(&mut self) -> Result<(), SpinelDBError> {
        // Extract current replication config.
        let (host, port, tls_enabled, my_port) = {
            let config_guard = self.state.config.lock().await;
            match &config_guard.replication {
                ReplicationConfig::Replica {
                    primary_host,
                    primary_port,
                    tls_enabled,
                } => (
                    primary_host.clone(),
                    *primary_port,
                    *tls_enabled,
                    config_guard.port,
                ),
                _ => {
                    return Err(SpinelDBError::Internal(
                        "Replica worker running with non-replica config.".into(),
                    ));
                }
            }
        };

        // Reset local transaction state for the new connection.
        self.is_in_transaction = false;
        self.queued_tx_commands.clear();

        // Connect to the primary.
        let addr = format!("{host}:{port}");
        info!("Attempting to connect to primary at {}", addr);
        let tcp_stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| SpinelDBError::ReplicationError(format!("Failed to connect: {e}")))?;

        // Optionally perform a TLS handshake.
        let stream: ReplicaStream = if tls_enabled {
            info!("Establishing TLS connection with primary at {addr}");
            let mut root_cert_store = rustls::RootCertStore::empty();
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(tls_config));

            let domain = rustls::pki_types::ServerName::try_from(host.as_str())
                .map_err(|_| SpinelDBError::ReplicationError("Invalid TLS domain name".into()))?
                .to_owned();

            let tls_stream = connector.connect(domain, tcp_stream).await.map_err(|e| {
                SpinelDBError::ReplicationError(format!("TLS handshake failed: {e}"))
            })?;

            ReplicaStream::Tls(Box::new(tls_stream))
        } else {
            ReplicaStream::Tcp(tcp_stream)
        };

        info!("Successfully connected to primary. Starting handshake...");

        let (reader, mut writer) = split(stream);
        let mut framed_reader = FramedRead::new(reader, RespFrameCodec);

        // --- Step 1: Handshake ---
        let handshake_result = self
            .perform_handshake(&mut framed_reader, &mut writer, my_port)
            .await?;
        debug!("Handshake completed with result: {handshake_result:?}");

        // --- Step 2: Synchronization (Full or Partial) ---
        let mut final_reader = if handshake_result == HandshakeResult::FullResync {
            // A full resync involves receiving and loading an SPLDB snapshot.
            // The reader needs to be temporarily un-framed to read the raw binary data.
            let reader = framed_reader.into_inner();
            let mut buf_reader = TokioBufReader::new(reader);
            self.read_and_load_spldb(&mut buf_reader).await?;
            info!("Full resync successful. SPLDB loaded.");
            self.current_db_index = 0; // Reset DB context after loading snapshot.
            // Re-frame the reader to continue processing the command stream.
            FramedRead::new(buf_reader.into_inner(), RespFrameCodec)
        } else {
            // For a partial resync, we can continue using the existing framed reader.
            info!("Partial resync successful. Resuming command stream.");
            framed_reader
        };

        // --- Step 3: Live Command Stream Processing ---
        let writer_arc = Arc::new(Mutex::new(writer));
        self.process_command_stream(&mut final_reader, writer_arc)
            .await;

        Ok(())
    }

    async fn process_command_stream(
        &mut self,
        framed_reader: &mut FramedRead<ReadHalf<ReplicaStream>, RespFrameCodec>,
        writer: Arc<Mutex<WriteHalf<ReplicaStream>>>,
    ) {
        info!("Now in sync mode, processing command stream from primary.");
        while let Some(result) = framed_reader.next().await {
            match self.handle_primary_frame(result, &writer).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Error handling frame from primary: {e}. Disconnecting.");
                    self.is_in_transaction = false;
                    self.queued_tx_commands.clear();
                    break;
                }
            }
        }
    }

    async fn handle_primary_frame(
        &mut self,
        result: Result<RespFrame, SpinelDBError>,
        writer: &Arc<Mutex<WriteHalf<ReplicaStream>>>,
    ) -> Result<(), SpinelDBError> {
        let frame = result?;
        let frame_len = frame.encode_to_vec().unwrap_or_default().len() as u64;
        let command = Command::try_from(frame.clone())?;
        debug!("Received command from primary: {command:?}");

        self.apply_command_or_transaction(command.clone(), writer)
            .await?;

        if let Some(info) = self.state.replication.replica_info.lock().await.as_mut() {
            info.processed_offset += frame_len;
        }

        Ok(())
    }

    async fn apply_command_or_transaction(
        &mut self,
        command: Command,
        writer: &Arc<Mutex<WriteHalf<ReplicaStream>>>,
    ) -> Result<(), SpinelDBError> {
        if let Command::Select(Select { db_index }) = command {
            self.current_db_index = db_index;
            return Ok(());
        }

        match &command {
            Command::Multi => {
                if self.is_in_transaction {
                    return Err(SpinelDBError::ReplicationError(
                        "Nested MULTI received.".into(),
                    ));
                }
                self.is_in_transaction = true;
                self.queued_tx_commands.clear();
                return Ok(());
            }
            Command::Exec => {
                if !self.is_in_transaction {
                    return Err(SpinelDBError::ReplicationError(
                        "EXEC without MULTI.".into(),
                    ));
                }
                let commands = std::mem::take(&mut self.queued_tx_commands);
                self.is_in_transaction = false;
                self.apply_transaction(commands).await?;
                return Ok(());
            }
            Command::Discard => {
                if !self.is_in_transaction {
                    return Err(SpinelDBError::ReplicationError(
                        "DISCARD without MULTI.".into(),
                    ));
                }
                self.is_in_transaction = false;
                self.queued_tx_commands.clear();
                return Ok(());
            }
            _ => {}
        }
        if self.is_in_transaction {
            self.queued_tx_commands.push(command);
            return Ok(());
        }

        if let Command::Replconf(ref r) = command {
            if r.args
                .first()
                .is_some_and(|arg| arg.eq_ignore_ascii_case("GETACK"))
            {
                let offset = self
                    .state
                    .replication
                    .replica_info
                    .lock()
                    .await
                    .as_ref()
                    .map_or(0, |i| i.processed_offset);
                self.spawn_ack_task(writer.clone(), offset).await;
                return Ok(());
            }
        }

        self.apply_single_command(command).await
    }

    async fn apply_transaction(&mut self, commands: Vec<Command>) -> Result<(), SpinelDBError> {
        if commands.is_empty() {
            return Ok(());
        }
        info!(
            "Applying transaction with {} commands from primary.",
            commands.len()
        );
        let db = self
            .state
            .get_db(self.current_db_index)
            .ok_or_else(|| SpinelDBError::Internal("Replica using invalid DB index".into()))?;

        let all_keys: Vec<Bytes> = commands.iter().flat_map(|c| c.get_keys()).collect();
        let mut guards = db.lock_shards_for_keys(&all_keys).await;

        for command in &commands {
            if !command.get_flags().contains(CommandFlags::WRITE) {
                continue;
            }
            let mut ctx = ExecutionContext {
                state: self.state.clone(),
                locks: ExecutionLocks::Multi { guards },
                db: &db,
                command: Some(command.clone()),
                session_id: 0,
                authenticated_user: None,
            };
            match command.execute(&mut ctx).await {
                Ok(_) => {
                    guards = match ctx.locks {
                        ExecutionLocks::Multi { guards } => guards,
                        _ => unreachable!(),
                    };
                }
                Err(e) => {
                    let err_msg = format!(
                        "CRITICAL: Failed to execute command '{command:?}' in replicated transaction: {e}. Clearing local data."
                    );
                    error!("{}", err_msg);
                    self.clear_all_local_data().await;
                    *self.state.replication.replica_info.lock().await = None;
                    return Err(SpinelDBError::ReplicationError(err_msg));
                }
            }
        }
        info!("Successfully applied transaction from primary.");
        Ok(())
    }

    async fn apply_single_command(&mut self, command: Command) -> Result<(), SpinelDBError> {
        let db = self
            .state
            .get_db(self.current_db_index)
            .ok_or_else(|| SpinelDBError::Internal("Replica using invalid DB index".into()))?;

        if !command.get_flags().contains(CommandFlags::WRITE) {
            return Ok(());
        }

        let locks = db.determine_locks_for_command(&command).await;
        let mut ctx = ExecutionContext {
            state: self.state.clone(),
            locks,
            db: &db,
            command: Some(command.clone()),
            session_id: 0,
            authenticated_user: None,
        };

        if let Err(e) = command.execute(&mut ctx).await {
            let err_msg = format!(
                "CRITICAL: Failed to execute propagated command '{command:?}': {e}. Clearing local data."
            );
            error!("{}", err_msg);
            self.clear_all_local_data().await;
            *self.state.replication.replica_info.lock().await = None;
            Err(SpinelDBError::ReplicationError(err_msg))
        } else {
            Ok(())
        }
    }

    async fn clear_all_local_data(&mut self) {
        warn!("Clearing all data on this replica due to a critical replication error.");
        for db in &self.state.dbs {
            db.clear_all_shards().await;
        }
        self.current_db_index = 0;
    }

    async fn spawn_ack_task(&self, writer: Arc<Mutex<WriteHalf<ReplicaStream>>>, ack_offset: u64) {
        tokio::spawn(async move {
            let ack_cmd_frame = RespFrame::Array(vec![
                RespFrame::BulkString("REPLCONF".into()),
                RespFrame::BulkString("ACK".into()),
                RespFrame::BulkString(ack_offset.to_string().into()),
            ]);
            if let Ok(encoded) = ack_cmd_frame.encode_to_vec() {
                if let Err(e) = writer.lock().await.write_all(&encoded).await {
                    error!("Failed to send ACK to primary: {}", e);
                } else {
                    debug!("Sent ACK to primary with offset {}", ack_offset);
                }
            }
        });
    }

    async fn perform_handshake<R, W>(
        &mut self,
        framed: &mut FramedRead<R, RespFrameCodec>,
        writer: &mut W,
        my_port: u16,
    ) -> Result<HandshakeResult, SpinelDBError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        // Step 1: PING
        writer.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
        self.expect_simple_string(framed, "PONG").await?;
        info!("Handshake step 1/4 (PING) successful.");

        // Step 2: REPLCONF listening-port
        let replconf_port_cmd = RespFrame::Array(vec![
            RespFrame::BulkString("REPLCONF".into()),
            RespFrame::BulkString("listening-port".into()),
            RespFrame::BulkString(my_port.to_string().into()),
        ])
        .encode_to_vec()?;
        writer.write_all(&replconf_port_cmd).await?;
        self.expect_simple_string(framed, "OK").await?;
        info!("Handshake step 2/4 (REPLCONF port) successful.");

        // Step 3: REPLCONF capa psync2
        let replconf_capa_cmd = RespFrame::Array(vec![
            RespFrame::BulkString("REPLCONF".into()),
            RespFrame::BulkString("capa".into()),
            RespFrame::BulkString("psync2".into()),
        ])
        .encode_to_vec()?;
        writer.write_all(&replconf_capa_cmd).await?;
        self.expect_simple_string(framed, "OK").await?;
        info!("Handshake step 3/4 (REPLCONF capa) successful.");

        // Step 4: PSYNC
        let (replid, offset) = self
            .state
            .replication
            .replica_info
            .lock()
            .await
            .as_ref()
            .map_or(("?".to_string(), "-1".to_string()), |i| {
                (i.master_replid.clone(), i.processed_offset.to_string())
            });
        info!("Handshake step 4/4: Sending PSYNC with id '{replid}' and offset '{offset}'.");
        let psync_cmd = RespFrame::Array(vec![
            RespFrame::BulkString("PSYNC".into()),
            RespFrame::BulkString(replid.into()),
            RespFrame::BulkString(offset.into()),
        ])
        .encode_to_vec()?;
        writer.write_all(&psync_cmd).await?;

        // Process the PSYNC response.
        let sync_response = framed.next().await.ok_or_else(|| {
            SpinelDBError::ReplicationError("Connection closed during PSYNC".into())
        })??;
        if let RespFrame::SimpleString(s) = sync_response {
            if s.starts_with("FULLRESYNC") {
                let new_master_run_id = self.handle_fullresync_response(&s).await?;

                let now_unix_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                self.state
                    .replication
                    .poisoned_masters
                    .retain(|_, expiry| *expiry > now_unix_secs);

                if let Some(expiry_timestamp) = self
                    .state
                    .replication
                    .poisoned_masters
                    .get(&new_master_run_id)
                {
                    if *expiry_timestamp.value() > now_unix_secs {
                        return Err(SpinelDBError::ReplicationError(format!(
                            "Refusing to sync with a poisoned master: {new_master_run_id}"
                        )));
                    }
                }

                Ok(HandshakeResult::FullResync)
            } else if s.eq_ignore_ascii_case("CONTINUE") {
                Ok(HandshakeResult::PartialResync)
            } else {
                Err(SpinelDBError::ReplicationError(format!(
                    "Unexpected PSYNC response: {s}"
                )))
            }
        } else {
            Err(SpinelDBError::ReplicationError(format!(
                "Expected Simple String for PSYNC, got {sync_response:?}"
            )))
        }
    }

    async fn handle_fullresync_response(
        &mut self,
        response_str: &str,
    ) -> Result<String, SpinelDBError> {
        let parts: Vec<&str> = response_str.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(SpinelDBError::ReplicationError(
                "Invalid FULLRESYNC format".into(),
            ));
        }
        let new_replid = parts[1].to_string();
        let master_offset: u64 = parts[2]
            .parse()
            .map_err(|_| SpinelDBError::ReplicationError("Invalid offset in FULLRESYNC".into()))?;
        info!(
            "Primary ordered full resync. New master replid: {new_replid}. Master offset: {master_offset}"
        );
        *self.state.replication.replica_info.lock().await = Some(ReplicaInfo {
            master_replid: new_replid.clone(),
            processed_offset: master_offset,
        });
        Ok(new_replid)
    }

    async fn read_and_load_spldb<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut TokioBufReader<R>,
    ) -> Result<(), SpinelDBError> {
        let mut line_buf = String::new();
        reader.read_line(&mut line_buf).await?;
        if !line_buf.starts_with('$') {
            return Err(SpinelDBError::ReplicationError(format!(
                "Expected SPLDB length prefix ('$'), got: {}",
                line_buf.trim()
            )));
        }
        let len_str = line_buf.trim_start_matches('$').trim_end_matches("\r\n");
        let spldb_len: usize = len_str.parse().map_err(|_| {
            SpinelDBError::ReplicationError(format!("Invalid SPLDB length: {len_str}"))
        })?;

        info!("Receiving SPLDB file of size: {spldb_len} bytes. Loading into DB...");
        let mut spldb_bytes = BytesMut::with_capacity(spldb_len);
        spldb_bytes.resize(spldb_len, 0);
        reader.read_exact(&mut spldb_bytes).await?;

        load_from_bytes(&spldb_bytes.freeze(), &self.state.dbs)
            .await
            .map_err(|e| SpinelDBError::ReplicationError(format!("SPLDB loading failed: {e}")))?;
        info!("Finished loading SPLDB data from primary.");
        Ok(())
    }

    async fn expect_simple_string<R: AsyncRead + Unpin>(
        &self,
        framed: &mut FramedRead<R, RespFrameCodec>,
        expected: &str,
    ) -> Result<(), SpinelDBError> {
        let frame = framed.next().await.ok_or_else(|| {
            SpinelDBError::ReplicationError("Connection closed during handshake".into())
        })??;
        match frame {
            RespFrame::SimpleString(s) if s.eq_ignore_ascii_case(expected) => Ok(()),
            RespFrame::Error(e) => Err(SpinelDBError::ReplicationError(format!(
                "Primary returned error: {e}"
            ))),
            _ => Err(SpinelDBError::ReplicationError(format!(
                "Expected '{expected}', got: {frame:?}"
            ))),
        }
    }
}
