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

    /// If `true`, enables the built-in, replica-initiated failover mechanism.
    /// WARNING: This is susceptible to split-brain during network partitions and is
    /// NOT recommended for production. Use the external Warden process for safe failover.
    #[serde(default)]
    pub replica_initiated_failover: bool,
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
            replica_initiated_failover: false, // Default to OFF for production safety.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_disabled() {
        let c = ClusterConfig::default();
        assert!(!c.enabled);
        assert!(!c.replica_initiated_failover);
    }

    #[test]
    fn test_default_file_path() {
        let c = ClusterConfig::default();
        assert_eq!(c.config_file, "nodes.conf");
    }

    #[test]
    fn test_default_node_timeout_is_15_seconds() {
        let c = ClusterConfig::default();
        assert_eq!(c.node_timeout, 15_000);
    }

    #[test]
    fn test_default_bus_port_offset() {
        let c = ClusterConfig::default();
        assert_eq!(c.bus_port_offset, 10_000);
    }

    #[test]
    fn test_default_failover_quorum() {
        let c = ClusterConfig::default();
        assert_eq!(c.failover_quorum, 2);
    }

    #[test]
    fn test_announce_fields_default_to_none() {
        let c = ClusterConfig::default();
        assert!(c.announce_ip.is_none());
        assert!(c.announce_port.is_none());
        assert!(c.announce_bus_port.is_none());
    }

    #[test]
    fn test_serde_roundtrip() {
        let c = ClusterConfig {
            enabled: true,
            config_file: "x.conf".to_string(),
            node_timeout: 5000,
            announce_ip: Some("10.0.0.1".to_string()),
            announce_port: Some(6379),
            announce_bus_port: Some(16379),
            bus_port_offset: 5000,
            failover_quorum: 3,
            replica_initiated_failover: true,
        };
        let s = toml::to_string(&c).unwrap();
        let d: ClusterConfig = toml::from_str(&s).unwrap();
        assert_eq!(d.enabled, c.enabled);
        assert_eq!(d.config_file, c.config_file);
        assert_eq!(d.node_timeout, c.node_timeout);
        assert_eq!(d.announce_ip, c.announce_ip);
        assert_eq!(d.announce_port, c.announce_port);
        assert_eq!(d.announce_bus_port, c.announce_bus_port);
        assert_eq!(d.bus_port_offset, c.bus_port_offset);
        assert_eq!(d.failover_quorum, c.failover_quorum);
        assert_eq!(d.replica_initiated_failover, c.replica_initiated_failover);
    }
}
