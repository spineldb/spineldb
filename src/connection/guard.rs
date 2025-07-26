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
        if self.is_handed_off {
            debug!(
                "ConnectionGuard for {} is being dropped, but cleanup is skipped due to handoff.",
                self.addr
            );
            return;
        }

        metrics::CONNECTED_CLIENTS.dec();
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
    }
}
