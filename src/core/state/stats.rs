// src/core/state/stats.rs

//! Contains state definitions and logic for server statistics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Holds all state and logic related to server-wide statistics and monitoring.
#[derive(Debug)]
pub struct StatsState {
    /// The total number of connections accepted by the server since startup.
    total_connections: AtomicU64,
    /// The total number of commands processed by the server since startup.
    total_commands: AtomicU64,
}

impl Default for StatsState {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsState {
    /// Creates a new `StatsState` with initialized counters.
    pub fn new() -> Self {
        Self {
            total_connections: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
        }
    }

    /// Atomically increments the total number of connections received.
    pub fn increment_total_connections(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Gets the total number of connections received.
    pub fn get_total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    /// Atomically increments the total number of commands processed.
    pub fn increment_total_commands(&self) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    /// Gets the total number of commands processed.
    pub fn get_total_commands(&self) -> u64 {
        self.total_commands.load(Ordering::Relaxed)
    }
}
