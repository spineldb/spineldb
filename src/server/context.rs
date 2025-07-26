// src/server/context.rs

use crate::core::state::{ServerInit, ServerState};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

/// Holds all the initialized state required to run the server's main loop.
pub struct ServerContext {
    pub state: Arc<ServerState>,
    pub init_channels: ServerInit,
    pub listener: TcpListener,
    pub shutdown_tx: broadcast::Sender<()>,
    pub background_tasks: JoinSet<Result<(), anyhow::Error>>,
    pub acceptor: Option<TlsAcceptor>,
}
