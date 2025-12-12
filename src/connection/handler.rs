// src/connection/handler.rs

//! Defines the `ConnectionHandler` which manages the full lifecycle of a client connection.

use super::guard::ConnectionGuard;
use super::session::SessionState;
use crate::core::handler::command_router::{RouteResponse, Router};
use crate::core::protocol::{RespFrame, RespFrameCodec};
use crate::core::pubsub::handler::PubSubModeHandler;
use crate::core::replication::handler::ReplicaHandler;
use crate::core::state::{ClientRole, ServerState};
use crate::core::{Command, SpinelDBError};
use crate::server::AnyStream;
use futures::{SinkExt, StreamExt, stream};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_rustls::server::TlsStream;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

/// The role of the connection, either a regular client or a replica.
#[derive(Clone, Copy, Debug, PartialEq)]
enum ConnectionRole {
    Client,
    ReplicaHandler,
}

/// The next step for the connection's main loop to take.
enum NextAction {
    Continue,
    EnterPubSub,
    ExitLoop,
}

/// Manages the full lifecycle of a client connection.
pub struct ConnectionHandler {
    framed: Option<Framed<AnyStream, RespFrameCodec>>,
    addr: SocketAddr,
    state: Arc<ServerState>,
    session_id: u64,
    shutdown_rx: broadcast::Receiver<()>,
    global_shutdown_rx: broadcast::Receiver<()>,
    session: SessionState,
    role: ConnectionRole,
}

impl ConnectionHandler {
    /// Creates a new `ConnectionHandler`.
    pub async fn new(
        socket: AnyStream,
        addr: SocketAddr,
        state: Arc<ServerState>,
        session_id: u64,
        shutdown_rx: broadcast::Receiver<()>,
        global_shutdown_rx: broadcast::Receiver<()>,
    ) -> Self {
        let is_auth_required = state.config.lock().await.password.is_some();
        let acl_enabled = state.acl_config.read().await.enabled;
        Self {
            framed: Some(Framed::new(socket, RespFrameCodec)),
            addr,
            state,
            session_id,
            shutdown_rx,
            global_shutdown_rx,
            session: SessionState::new(is_auth_required, acl_enabled),
            role: ConnectionRole::Client,
        }
    }

    /// The main event loop for the connection, handling incoming frames and signals.
    pub async fn run(&mut self) -> Result<(), SpinelDBError> {
        let mut guard = ConnectionGuard::new(self.state.clone(), self.session_id, self.addr);
        'main_loop: loop {
            if self.framed.is_none() {
                // Connection was handed off. Wait for global shutdown.
                self.global_shutdown_rx.recv().await.ok();
                break 'main_loop;
            }

            tokio::select! {
                // Prioritize shutdown signals over other events.
                biased;
                _ = self.global_shutdown_rx.recv() => {
                    info!("Connection handler for {} received GLOBAL shutdown signal.", self.addr);
                    if let Some(framed) = self.framed.as_mut() {
                        let shutdown_msg = RespFrame::Error("SHUTDOWN Server is shutting down".to_string());
                        let _ = framed.send(shutdown_msg).await;
                    }
                    break 'main_loop;
                }
                _ = self.shutdown_rx.recv() => {
                    info!("Connection handler for {} received kill signal.", self.addr);
                    break 'main_loop;
                }
                result = self.framed.as_mut().unwrap().next() => {
                    match result {
                        Some(Ok(frame)) => {
                            debug!("Session {}: Received frame: {:?}", self.session_id, frame);
                            match self.process_frame(frame, &mut guard).await {
                                Ok(NextAction::Continue) => {
                                    self.update_client_last_activity().await;
                                }
                                Ok(NextAction::EnterPubSub) => {
                                    if self.run_pubsub_mode().await.is_err() {
                                        break 'main_loop;
                                    }
                                }
                                Ok(NextAction::ExitLoop) => {
                                    break 'main_loop;
                                }
                                Err(e) => {
                                    // If an error occurs while in a transaction, the transaction must be aborted.
                                    // This handles errors from ACL, cluster redirection, OOM, etc.
                                    if self.session.is_in_transaction
                                        && let Some(db) = self.state.get_db(self.session.current_db_index)
                                            && let Some(mut tx_state) = db.tx_states.get_mut(&self.session_id) {
                                                tx_state.has_error = true;
                                            }
                                    self.send_error_to_client(e).await?;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            if is_normal_disconnect(&e) {
                                debug!("Connection from {} closed by peer: {}", self.addr, e);
                            } else {
                                warn!("Connection error for {}: {}", self.addr, e);
                            }
                            break 'main_loop;
                        }
                        None => {
                            debug!("Connection from {} closed by peer.", self.addr);
                            break 'main_loop;
                        }
                    }
                }
            }
        }

        // Clean up any lingering transaction state if the connection was not handed off.
        if !guard.is_handed_off
            && let Some(db) = self.state.get_db(self.session.current_db_index)
            && db.discard_transaction(self.session_id).is_ok()
        {
            debug!(
                "Cleaned up lingering transaction for client {} in DB {}.",
                self.addr, self.session.current_db_index
            );
        }
        Ok(())
    }

    /// Parses a RESP frame, routes it as a command, and sends the response.
    async fn process_frame(
        &mut self,
        frame: RespFrame,
        conn_guard: &mut ConnectionGuard,
    ) -> Result<NextAction, SpinelDBError> {
        let command_result = Command::try_from(frame);

        // If command parsing itself fails, mark the transaction as errored.
        if let Err(e) = &command_result {
            if self.session.is_in_transaction
                && let Some(db) = self.state.get_db(self.session.current_db_index)
                && let Some(mut tx_state) = db.tx_states.get_mut(&self.session_id)
            {
                tx_state.has_error = true;
            }
            return Err(e.clone());
        }

        let command = command_result.unwrap();
        debug!(
            "Session {}: Received command: {}",
            self.session_id,
            command.name()
        );

        // PSYNC is a special command that triggers a protocol switch and handoff.
        if let Command::Psync(psync) = command {
            return self.handle_replica_handoff(psync, conn_guard).await;
        }

        let mut router = Router::new(
            self.state.clone(),
            self.session_id,
            self.addr,
            &mut self.session,
        );
        let route_response = router.route(command).await?;
        let framed = self.framed.as_mut().unwrap();

        match route_response {
            RouteResponse::Single(response) => {
                debug!(
                    "Session {}: Sending single response: {:?}",
                    self.session_id, response
                );
                framed.send(response.into()).await?;
            }
            RouteResponse::Multiple(responses) => {
                debug!(
                    "Session {}: Sending multiple responses: {:?}",
                    self.session_id, responses
                );
                let mut stream = stream::iter(responses).map(|r| Ok(r.into()));
                framed.send_all(&mut stream).await?;
            }
            RouteResponse::StreamBody {
                resp_header,
                mut file,
                ..
            } => {
                debug!(
                    "Session {}: Streaming file with header: {:?}",
                    self.session_id,
                    String::from_utf8_lossy(&resp_header)
                );
                let stream = framed.get_mut();
                stream.write_all(&resp_header).await?;
                tokio::io::copy(&mut file, stream).await?;
                stream.write_all(b"\r\n").await?;
                stream.flush().await?;
            }
            RouteResponse::NoOp => {
                debug!(
                    "Session {}: No operation, not sending response.",
                    self.session_id
                );
            }
        }

        // If SUBSCRIBE or PSUBSCRIBE was successful, transition to Pub/Sub mode.
        if self.session.is_subscribed || self.session.is_pattern_subscribed {
            Ok(NextAction::EnterPubSub)
        } else {
            Ok(NextAction::Continue)
        }
    }

    /// Hands off the connection to a dedicated `ReplicaHandler`.
    async fn handle_replica_handoff(
        &mut self,
        psync: crate::core::commands::generic::Psync,
        conn_guard: &mut ConnectionGuard,
    ) -> Result<NextAction, SpinelDBError> {
        self.role = ConnectionRole::ReplicaHandler;

        // Update the client info to reflect its new role as a replica.
        if let Some(entry) = self.state.clients.get(&self.session_id) {
            let (client_info_arc, _) = entry.value();
            client_info_arc.lock().await.role = ClientRole::Replica;
        }

        conn_guard.set_handed_off();

        // Explicitly discard any lingering transaction state before handoff.
        if self.session.is_in_transaction
            && let Some(db) = self.state.get_db(self.session.current_db_index)
            && db.discard_transaction(self.session_id).is_ok()
        {
            debug!(
                "Cleaned up lingering transaction for client {} before replica handoff.",
                self.addr
            );
        }

        info!("Handing off connection {} to ReplicaHandler.", self.addr);
        let shutdown_rx_for_handler = self.shutdown_rx.resubscribe();

        let Some(framed) = self.framed.take() else {
            return Err(SpinelDBError::Internal(
                "Framed stream already taken for replica handoff".into(),
            ));
        };

        let any_stream = framed.into_inner();
        match any_stream {
            AnyStream::Tls(tls_stream) => {
                info!("Handoff: Detected TLS stream for replication.");
                let handler = ReplicaHandler::<TlsStream<TcpStream>>::new(
                    self.state.clone(),
                    self.addr,
                    *tls_stream,
                );
                tokio::spawn(handler.run(
                    psync.replication_id,
                    psync.offset,
                    shutdown_rx_for_handler,
                ));
            }
            AnyStream::Tcp(tcp_stream) => {
                info!("Handoff: Detected plain TCP stream for replication.");
                let handler =
                    ReplicaHandler::<TcpStream>::new(self.state.clone(), self.addr, tcp_stream);
                tokio::spawn(handler.run(
                    psync.replication_id,
                    psync.offset,
                    shutdown_rx_for_handler,
                ));
            }
        }
        Ok(NextAction::ExitLoop)
    }

    /// Transitions this handler to Pub/Sub mode.
    async fn run_pubsub_mode(&mut self) -> Result<(), SpinelDBError> {
        let framed = self.framed.as_mut().ok_or_else(|| {
            SpinelDBError::Internal("Cannot enter pubsub mode without a framed stream".into())
        })?;
        let mut pubsub_handler = PubSubModeHandler::new(
            framed,
            &mut self.shutdown_rx,
            &mut self.session,
            self.state.clone(),
        );
        let result = pubsub_handler.run().await;

        // Clean up all subscription state upon exiting Pub/Sub mode.
        self.session.is_subscribed = false;
        self.session.is_pattern_subscribed = false;
        self.session.subscribed_channels.clear();
        self.session.subscribed_patterns.clear();
        self.session.pubsub_receivers.clear();
        result
    }

    /// Sends an error frame back to the client.
    async fn send_error_to_client(&mut self, e: SpinelDBError) -> Result<(), SpinelDBError> {
        if let Some(framed) = self.framed.as_mut() {
            let error_frame = RespFrame::Error(e.to_string());
            debug!(
                "Session {}: Sending error response: {:?}",
                self.session_id, error_frame
            );
            framed.send(error_frame).await?;
        }
        Ok(())
    }

    /// Updates the client's last activity time for monitoring (`CLIENT LIST`).
    async fn update_client_last_activity(&self) {
        if let Some(entry) = self.state.clients.get(&self.session_id) {
            entry.value().0.lock().await.last_command_time = Instant::now();
        }
    }
}

/// Helper function to check for non-critical disconnection errors.
fn is_normal_disconnect(e: &SpinelDBError) -> bool {
    matches!(e, SpinelDBError::Io(arc_err) if matches!(
        arc_err.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionAborted
    ))
}
