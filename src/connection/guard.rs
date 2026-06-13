// src/connection/guard.rs

//! Defines `ConnectionGuard`, an RAII guard for connection resource management.

use crate::core::metrics;
use crate::core::state::ServerState;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::debug;

/// An RAII guard to ensure connection resources are always cleaned up when a
/// connection handler's scope is exited.
pub struct ConnectionGuard {
    /// A shared reference to the server state.
    pub(crate) state: Arc<ServerState>,
    /// The unique identifier for the client session.
    pub(crate) session_id: u64,
    /// The network address of the client.
    pub(crate) addr: SocketAddr,
    /// A flag to prevent cleanup if the connection was handed off to another handler
    /// (e.g., for replication).
    pub(crate) is_handed_off: bool,
}

impl ConnectionGuard {
    /// Creates a new `ConnectionGuard`.
    pub(crate) fn new(state: Arc<ServerState>, session_id: u64, addr: SocketAddr) -> Self {
        Self {
            state,
            session_id,
            addr,
            is_handed_off: false,
        }
    }

    /// Marks the connection as handed off, skipping cleanup in this guard's
    /// `Drop` implementation.
    pub(crate) fn set_handed_off(&mut self) {
        self.is_handed_off = true;
    }
}

impl Drop for ConnectionGuard {
    /// Performs resource cleanup when the guard goes out of scope.
    /// This includes removing the client from global maps and cleaning up any
    /// pending blockers.
    fn drop(&mut self) {
        // Always decrement the connected clients metric, even for handed-off connections.
        metrics::CONNECTED_CLIENTS.dec();

        if self.is_handed_off {
            debug!(
                "ConnectionGuard for {} is being dropped, but cleanup is skipped due to handoff.",
                self.addr
            );
            return;
        }

        debug!(
            "ConnectionGuard dropping, cleaning up resources for connection {}",
            self.addr
        );

        // Remove the client from the central client map.
        if self.state.clients.remove(&self.session_id).is_none() {
            debug!(
                "Client {} was not in the global state map upon cleanup (likely a replica).",
                self.addr
            );
        }

        // Clean up any potential lingering state from blocking commands.
        self.state
            .blocker_manager
            .remove_waiters_for_session(self.session_id);
        self.state
            .stream_blocker_manager
            .remove_waiters_for_session(self.session_id);

        // Clean up CLIENT TRACKING state for this session.
        self.state.tracking.cleanup_session(self.session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::test_helpers::init_server_state;
    use std::net::SocketAddr;

    #[test]
    fn test_connection_guard_new_creates_with_defaults() {
        let state = init_server_state(Config::default());
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let guard = ConnectionGuard::new(state, 42, addr);
        assert_eq!(guard.session_id, 42);
        assert_eq!(guard.addr, addr);
        assert!(!guard.is_handed_off);
    }

    #[test]
    fn test_set_handed_off_flag() {
        let state = init_server_state(Config::default());
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let mut guard = ConnectionGuard::new(state, 1, addr);
        assert!(!guard.is_handed_off);
        guard.set_handed_off();
        assert!(guard.is_handed_off);
    }

    #[test]
    fn test_guard_drop_removes_client_from_map() {
        let state = init_server_state(Config::default());
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let session_id = 99u64;

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let client_info = Arc::new(tokio::sync::Mutex::new(crate::core::state::ClientInfo {
            addr,
            session_id,
            name: None,
            db_index: 0,
            role: crate::core::state::ClientRole::Normal,
            created: std::time::Instant::now(),
            last_command_time: std::time::Instant::now(),
            library_name: None,
            library_version: None,
            no_evict: false,
            no_touch: false,
            protocol_version: 3,
        }));
        state.clients.insert(session_id, (client_info, shutdown_tx));

        assert!(state.clients.contains_key(&session_id));

        {
            let _guard = ConnectionGuard::new(state.clone(), session_id, addr);
            assert!(state.clients.contains_key(&session_id));
        }
        assert!(!state.clients.contains_key(&session_id));
    }

    #[test]
    fn test_guard_drop_skips_cleanup_when_handed_off() {
        let state = init_server_state(Config::default());
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let session_id = 100u64;

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let client_info = Arc::new(tokio::sync::Mutex::new(crate::core::state::ClientInfo {
            addr,
            session_id,
            name: None,
            db_index: 0,
            role: crate::core::state::ClientRole::Normal,
            created: std::time::Instant::now(),
            last_command_time: std::time::Instant::now(),
            library_name: None,
            library_version: None,
            no_evict: false,
            no_touch: false,
            protocol_version: 3,
        }));
        state.clients.insert(session_id, (client_info, shutdown_tx));

        {
            let mut guard = ConnectionGuard::new(state.clone(), session_id, addr);
            guard.set_handed_off();
        }
        assert!(state.clients.contains_key(&session_id));
        state.clients.remove(&session_id);
    }
}
