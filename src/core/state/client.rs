// src/core/state/client.rs

//! Contains state definitions related to client connections.

use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, broadcast};

pub type ShutdownSender = broadcast::Sender<()>;
pub type ClientStateTuple = (Arc<Mutex<ClientInfo>>, ShutdownSender);
pub type ClientMap = Arc<DashMap<u64, ClientStateTuple>>;

/// Represents the role of the connection from the primary server's perspective.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClientRole {
    /// A normal client connection.
    Normal,
    /// A connection that has been handed off for replication.
    Replica,
}

#[derive(Debug)]
pub struct ClientInfo {
    pub addr: SocketAddr,
    pub session_id: u64,
    pub name: Option<String>,
    pub db_index: usize,
    /// The role of this connection (e.g., normal client or replica).
    pub role: ClientRole,
    pub created: Instant,
    pub last_command_time: Instant,
}
