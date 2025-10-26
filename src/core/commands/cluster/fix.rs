// src/core/commands/cluster/fix.rs

use crate::core::cluster::client::ClusterClient;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::state::ServerState;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use anyhow::anyhow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};

/// Executes the `CLUSTER FIX` command.
/// This command attempts to resolve stuck `MIGRATING` or `IMPORTING` slots across the cluster.
pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    info!("CLUSTER FIX command initiated.");
    let state_clone = ctx.state.clone();

    // The orchestrator logic is complex and involves network I/O.
    // It's better to handle its specific errors rather than spawning it as a detached task.
    match run_fix_orchestrator(state_clone).await {
        Ok(log) => {
            let log_string = log.join("\n");
            Ok((
                RespValue::BulkString(log_string.into()),
                // Considered a write as it modifies cluster state on other nodes.
                WriteOutcome::Write { keys_modified: 0 },
            ))
        }
        Err(e) => Err(SpinelDBError::Internal(format!("CLUSTER FIX failed: {e}"))),
    }
}

/// The main orchestrator that scans the cluster state for inconsistencies and attempts to fix them.
/// This implementation is robust, efficient, and authoritative.
async fn run_fix_orchestrator(state: Arc<ServerState>) -> Result<Vec<String>, anyhow::Error> {
    let cluster = state
        .cluster
        .as_ref()
        .ok_or_else(|| anyhow!("Not in cluster mode"))?;
    let mut log = Vec::new();

    // --- Phase 1: Discovery ---
    // Identify all stuck slots and the nodes involved without taking a long-lived snapshot.
    info!("[CLUSTER FIX] Phase 1: Discovering stuck slots...");
    let mut stuck_slots = BTreeMap::new(); // K: slot, V: (source_id, dest_id)
    let mut involved_node_ids = HashSet::new();

    for entry in cluster.nodes.iter() {
        let node_id = entry.key();
        let node_info = &entry.value().node_info;
        for (slot, dest_id) in &node_info.migrating_slots {
            stuck_slots.insert(*slot, (node_id.clone(), dest_id.clone()));
            involved_node_ids.insert(node_id.clone());
            involved_node_ids.insert(dest_id.clone());
        }
        for (slot, source_id) in &node_info.importing_slots {
            stuck_slots.insert(*slot, (source_id.clone(), node_id.clone()));
            involved_node_ids.insert(source_id.clone());
            involved_node_ids.insert(node_id.clone());
        }
    }

    if stuck_slots.is_empty() {
        info!("[CLUSTER FIX] No stuck slots found. Nothing to do.");
        log.push("No stuck slots found.".to_string());
        return Ok(log);
    }
    log.push(format!("Found {} stuck slots to fix.", stuck_slots.len()));

    // --- Phase 2: Resolution & Connection ---
    // Decide on the correct owner for each slot and establish connections.
    info!("[CLUSTER FIX] Phase 2: Resolving ownership and connecting to nodes...");
    let mut resolved_owners = BTreeMap::new(); // K: slot, V: owner_id
    for (slot, (source_id, _)) in &stuck_slots {
        // The safest resolution for FIX is to revert ownership to the source node.
        resolved_owners.insert(*slot, source_id.clone());
    }

    let mut clients = HashMap::new();
    for node_id in &involved_node_ids {
        if let Some(node_runtime) = cluster.nodes.get(node_id) {
            let addr_str = &node_runtime.value().node_info.addr;
            let addr: SocketAddr = addr_str.parse()?;
            match ClusterClient::connect(addr).await {
                Ok(client) => {
                    clients.insert(node_id.clone(), client);
                    info!("-> Connected to node {} ({})", node_id, addr);
                }
                Err(e) => {
                    warn!("-> FAILED to connect to node {} ({}): {}", node_id, addr, e);
                    log.push(format!(
                        "Warning: Could not connect to node {node_id}. It may not receive the fix."
                    ));
                }
            }
        }
    }

    // --- Phase 3: Authoritative Execution ---
    // Broadcast the resolved state to all reachable nodes involved in the migrations.
    info!("[CLUSTER FIX] Phase 3: Broadcasting authoritative state to all nodes...");
    for (slot, owner_id) in &resolved_owners {
        log.push(format!(
            "-> Fixing slot {}: assigning ownership back to node {}",
            slot, owner_id
        ));
        let setslot_args = vec![
            "SETSLOT".into(),
            slot.to_string().into(),
            "NODE".into(),
            owner_id.clone().into(),
        ];
        for (node_id, client) in clients.iter_mut() {
            if let Err(e) = client.cluster_setslot(setslot_args.clone()).await {
                warn!("-> FAILED to send SETSLOT NODE to {}: {}", node_id, e);
                log.push(format!(
                    "Warning: Failed to fix slot {slot} on node {node_id}: {e}"
                ));
            }
        }
    }

    // --- Phase 4: Finalization ---
    // Update the local state directly and save it to disk.
    info!("[CLUSTER FIX] Phase 4: Updating local state and saving configuration...");
    for (slot, owner_id) in resolved_owners {
        // Clear migration state from all local node entries
        for mut node in cluster.nodes.iter_mut() {
            node.value_mut().node_info.migrating_slots.remove(&slot);
            node.value_mut().node_info.importing_slots.remove(&slot);
        }
        // Assign final ownership locally
        *cluster.slots_map[slot as usize].write() = Some(owner_id);
    }
    // Update slot ownership counts on each node's info struct
    for mut node in cluster.nodes.iter_mut() {
        node.value_mut().node_info.slots.clear();
    }
    for (slot, owner_id) in cluster.slots_map.iter().enumerate() {
        if let Some(id) = owner_id.read().as_ref()
            && let Some(mut node) = cluster.nodes.get_mut(id)
        {
            node.value_mut().node_info.slots.insert(slot as u16);
        }
    }

    cluster.save_config().await?;
    log.push("Successfully updated local configuration and saved to nodes.conf.".to_string());
    info!("[CLUSTER FIX] Process complete.");

    Ok(log)
}
