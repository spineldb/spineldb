// src/core/commands/cluster/replicate.rs

//! Implements the `CLUSTER REPLICATE <master-id>` command.
//! This command reconfigures a replica node to replicate a new master within the cluster.

use crate::core::cluster::NodeFlags;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use tracing::{info, warn};

/// Executes the `CLUSTER REPLICATE <master-id>` command.
/// This command reconfigures a replica node to follow a new master.
pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    master_id: &str,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx.state.cluster.as_ref().unwrap();

    // --- Pre-flight Checks ---

    // 1. Prevent a node from replicating itself.
    if master_id == cluster.my_id {
        return Err(SpinelDBError::ReplicationLoopDetected);
    }

    // 2. Detect circular replication chains (e.g., A -> B -> A).
    // This loop traverses the entire replication chain of the target master.
    // If our own node ID is found anywhere in that chain, it would create a loop.
    let mut current_id = master_id.to_string();
    while let Some(node_entry) = cluster.nodes.get(&current_id) {
        if let Some(next_master_id) = &node_entry.node_info.replica_of {
            // If the next master in the chain is this node, we have a loop.
            if next_master_id == &cluster.my_id {
                return Err(SpinelDBError::ReplicationLoopDetected);
            }
            // Move up the chain for the next iteration.
            current_id = next_master_id.clone();
        } else {
            // Reached the top of the chain (a primary), no loop found.
            break;
        }
    }

    // --- Configuration Update ---

    // 3. Update the central server configuration (`Config` struct) to point
    //    to the new master's host and port. This is what the replication worker uses.
    {
        let mut config_guard = ctx.state.config.lock().await;
        if let crate::config::ReplicationConfig::Replica {
            primary_host,
            primary_port,
            ..
        } = &mut config_guard.replication
        {
            if let Some(master_node) = cluster.nodes.get(master_id) {
                // Parse the new master's address (ip:port).
                let parts: Vec<&str> = master_node.node_info.addr.split(':').collect();
                *primary_host = parts[0].to_string();
                *primary_port = parts
                    .get(1)
                    .and_then(|p_str| p_str.parse().ok())
                    .unwrap_or(0); // Default to 0 on parse error, though it shouldn't happen.
                info!(
                    "Updated replica config to follow new master {}",
                    master_node.node_info.addr
                );
            } else {
                return Err(SpinelDBError::InvalidState(format!(
                    "Master node {master_id} not found"
                )));
            }
        } else {
            // This command is only valid on a node configured as a replica.
            return Err(SpinelDBError::InvalidState(
                "This node is not a replica. Cannot reconfigure.".to_string(),
            ));
        }
    } // `config_guard` is dropped, releasing the lock.

    // 4. Update this node's role and master ID in the cluster state map.
    // This information is gossiped to other nodes and persisted in `nodes.conf`.
    let mut myself = cluster.nodes.get_mut(&cluster.my_id).unwrap();
    let mut flags = myself.node_info.get_flags();
    flags.remove(NodeFlags::PRIMARY);
    flags.insert(NodeFlags::REPLICA);
    myself.node_info.set_flags(flags);
    myself.node_info.replica_of = Some(master_id.to_string());
    // Persist the change to `nodes.conf` to make it durable across restarts.
    cluster.save_config().await?;

    // --- Trigger Reconfiguration ---

    // 5. Signal the replication worker to disconnect from the old master and
    //    reconnect to the new one using the updated config.
    if ctx.state.replication_reconfigure_tx.send(()).is_err() {
        warn!(
            "Could not send reconfigure signal to replication worker. It may not be running or the channel is full."
        );
    }

    info!(
        "This node is now configured as a replica of {} and reconfiguration has been triggered.",
        master_id
    );

    Ok((
        RespValue::SimpleString("OK".into()),
        // This is a configuration change, not a keyspace write, so it doesn't get
        // propagated via the standard AOF/replication mechanism.
        WriteOutcome::DidNotWrite,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    struct FakeReplicationCluster {
        my_id: String,
        nodes: HashMap<String, Option<String>>,
    }

    impl FakeReplicationCluster {
        fn detect_self_replication(&self, master_id: &str) -> bool {
            master_id == self.my_id
        }

        fn detect_circular_replication(&self, master_id: &str) -> bool {
            let mut current_id = master_id.to_string();
            while let Some(replica_of) = self.nodes.get(&current_id).and_then(|r| r.as_ref()) {
                if replica_of == &self.my_id {
                    return true;
                }
                current_id = replica_of.clone();
            }
            false
        }
    }

    #[test]
    fn test_self_replication_is_detected() {
        let cluster = FakeReplicationCluster {
            my_id: "node-1".to_string(),
            nodes: [("node-1".to_string(), None)].into(),
        };
        assert!(cluster.detect_self_replication("node-1"));
    }

    #[test]
    fn test_self_replication_not_false_positive() {
        let cluster = FakeReplicationCluster {
            my_id: "node-1".to_string(),
            nodes: [("node-2".to_string(), None)].into(),
        };
        assert!(!cluster.detect_self_replication("node-2"));
    }

    #[test]
    fn test_circular_replication_direct_loop() {
        let cluster = FakeReplicationCluster {
            my_id: "A".to_string(),
            nodes: [
                ("A".to_string(), None),
                ("B".to_string(), Some("A".to_string())),
            ]
            .into(),
        };
        assert!(cluster.detect_circular_replication("B"));
    }

    #[test]
    fn test_circular_replication_chain_loop() {
        let cluster = FakeReplicationCluster {
            my_id: "A".to_string(),
            nodes: [
                ("A".to_string(), None),
                ("B".to_string(), Some("C".to_string())),
                ("C".to_string(), Some("A".to_string())),
            ]
            .into(),
        };
        assert!(cluster.detect_circular_replication("B"));
    }

    #[test]
    fn test_no_circular_replication_chain_to_primary() {
        let cluster = FakeReplicationCluster {
            my_id: "A".to_string(),
            nodes: [
                ("A".to_string(), None),
                ("B".to_string(), Some("C".to_string())),
                ("C".to_string(), None),
            ]
            .into(),
        };
        assert!(!cluster.detect_circular_replication("B"));
    }

    #[test]
    fn test_no_circular_replication_unknown_node() {
        let cluster = FakeReplicationCluster {
            my_id: "A".to_string(),
            nodes: [("A".to_string(), None)].into(),
        };
        assert!(!cluster.detect_circular_replication("nonexistent"));
    }

    #[test]
    fn test_addr_parsing() {
        let addr = "192.168.1.100:7000";
        let parts: Vec<&str> = addr.split(':').collect();
        assert_eq!(parts[0], "192.168.1.100");
        assert_eq!(parts.get(1).and_then(|p| p.parse::<u16>().ok()), Some(7000));
    }

    #[test]
    fn test_addr_parsing_invalid_port() {
        let addr = "192.168.1.100:notaport";
        let parts: Vec<&str> = addr.split(':').collect();
        let port: Option<u16> = parts.get(1).and_then(|p| p.parse().ok());
        assert_eq!(port, None);
    }
}
