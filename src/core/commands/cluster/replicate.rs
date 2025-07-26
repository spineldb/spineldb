// src/core/commands/cluster/replicate.rs

//! Implements the `CLUSTER REPLICATE <master-id>` command.
//! This command reconfigures a replica node to replicate a new master within the cluster.

use crate::core::cluster::NodeFlags;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::storage::db::ExecutionContext;
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
    cluster.save_config()?;

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
