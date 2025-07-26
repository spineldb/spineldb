// src/core/cluster/mod.rs

//! This module contains all logic related to the cluster mode, including
//! state management, the gossip protocol, failover, and slot handling.

pub mod client;
pub mod config;
pub mod failover;
pub mod gossip;
pub mod secure_gossip;
pub mod slot;
pub mod state;

// Re-export key types for easier access from other modules.
pub use config::ClusterConfig;
pub use state::{ClusterNode, NodeFlags, NodeRole, NodeRuntimeState};
