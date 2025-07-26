// src/core/cluster/config.rs

//! Defines the cluster-specific configuration options.

use serde::{Deserialize, Serialize};

/// Holds all configuration settings related to cluster mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// If `true`, the server will start in cluster mode.
    #[serde(default)]
    pub enabled: bool,
    /// The path to the cluster configuration file (e.g., `nodes.conf`).
    #[serde(default = "default_config_file")]
    pub config_file: String,
    /// The timeout in milliseconds after which a node is considered to be in a
    /// PFAIL (Possible Failure) state if no PONG is received.
    #[serde(default = "default_node_timeout")]
    pub node_timeout: u64,
    /// An optional IP address to announce to other nodes in the cluster.
    pub announce_ip: Option<String>,
    /// An optional port to announce for client connections.
    pub announce_port: Option<u16>,
    /// An optional port to announce for the cluster bus communication.
    pub announce_bus_port: Option<u16>,
    /// The port offset for the cluster bus.
    /// The final bus port will be client_port + bus_port_offset.
    #[serde(default = "default_bus_port_offset")]
    pub bus_port_offset: u16,
    /// The number of master nodes that must be reachable for a master to remain writable.
    /// This is the primary mechanism to prevent split-brain during partitions.
    /// It should be set to (total_masters / 2) + 1.
    #[serde(default = "default_failover_quorum")]
    pub failover_quorum: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            config_file: "nodes.conf".to_string(),
            node_timeout: 15000,
            announce_ip: None,
            announce_port: None,
            announce_bus_port: None,
            bus_port_offset: 10000,
            failover_quorum: 2, // A safe default for a minimal 3-master setup.
        }
    }
}

fn default_config_file() -> String {
    "nodes.conf".to_string()
}
fn default_node_timeout() -> u64 {
    15000
}
fn default_bus_port_offset() -> u16 {
    10000
}
fn default_failover_quorum() -> usize {
    2
}
