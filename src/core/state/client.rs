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
    /// The name of the client library, set by CLIENT SETINFO.
    pub library_name: Option<String>,
    /// The version of the client library, set by CLIENT SETINFO.
    pub library_version: Option<String>,
    /// Whether this client is exempt from maxmemory eviction (CLIENT NO-EVICT).
    pub no_evict: bool,
    /// Whether this client bypasses LRU tracking for key accesses (CLIENT NO-TOUCH).
    pub no_touch: bool,
    /// The RESP protocol version used by this client (2 or 3).
    pub protocol_version: u8,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_client_info() -> ClientInfo {
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        ClientInfo {
            addr,
            session_id: 1,
            name: None,
            db_index: 0,
            role: ClientRole::Normal,
            created: Instant::now(),
            last_command_time: Instant::now(),
            library_name: None,
            library_version: None,
            no_evict: false,
            no_touch: false,
            protocol_version: 3,
        }
    }

    #[test]
    fn test_client_role_variants_distinct() {
        assert_ne!(ClientRole::Normal, ClientRole::Replica);
    }

    #[test]
    fn test_client_info_default_values() {
        let c = make_client_info();
        assert_eq!(c.addr.to_string(), "127.0.0.1:6379");
        assert_eq!(c.session_id, 1);
        assert!(c.name.is_none());
        assert_eq!(c.db_index, 0);
        assert_eq!(c.role, ClientRole::Normal);
        assert!(c.library_name.is_none());
        assert!(c.library_version.is_none());
    }

    #[test]
    fn test_client_info_role_can_be_replica() {
        let mut c = make_client_info();
        c.role = ClientRole::Replica;
        assert_eq!(c.role, ClientRole::Replica);
    }

    #[test]
    fn test_client_info_name_mutable() {
        let mut c = make_client_info();
        c.name = Some("my-client".to_string());
        c.library_name = Some("redis-rs".to_string());
        c.library_version = Some("1.0.0".to_string());
        c.db_index = 5;
        assert_eq!(c.name.as_deref(), Some("my-client"));
        assert_eq!(c.library_name.as_deref(), Some("redis-rs"));
        assert_eq!(c.library_version.as_deref(), Some("1.0.0"));
        assert_eq!(c.db_index, 5);
    }

    #[test]
    fn test_client_info_timestamps() {
        let c = make_client_info();
        // The `last_command_time` should be at or after `created`.
        assert!(c.last_command_time >= c.created);
        // Sleeping briefly should not affect the past.
        std::thread::sleep(Duration::from_millis(1));
        let new_time = Instant::now();
        assert!(new_time > c.created);
    }
}
