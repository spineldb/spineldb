// src/core/commands/cluster/reshard.rs

use crate::core::cluster::NodeFlags;
use crate::core::cluster::client::ClusterClient;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::state::ServerState;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use anyhow::anyhow;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Executes the CLUSTER RESHARD command by spawning a background orchestrator task.
pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    source_node_id: &str,
    destination_node_id: &str,
    slots: &[u16],
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    // Create two clones of the Arc. One for the task, one for accessing the JoinSet.
    let state_for_task = ctx.state.clone();
    let state_for_spawning = ctx.state.clone();

    let source_clone = source_node_id.to_owned();
    let dest_clone = destination_node_id.to_owned();
    let slots_clone = slots.to_vec();

    // The async block now moves `state_for_task`.
    let task = async move {
        info!(
            "Starting background resharding task: {:?} slots from {} to {}",
            slots_clone, source_clone, dest_clone
        );
        if let Err(e) =
            run_reshard_orchestrator(state_for_task, source_clone, dest_clone, slots_clone).await
        {
            error!("Resharding task failed: {}", e);
        } else {
            info!("Resharding task completed successfully.");
        }
    };

    // Use the second clone, `state_for_spawning`, to access the critical_tasks lock.
    state_for_spawning.critical_tasks.lock().await.spawn(task);

    // The command returns immediately with OK.
    Ok((
        RespValue::SimpleString("OK".into()),
        WriteOutcome::DidNotWrite,
    ))
}

/// The main resharding orchestrator. It connects to both nodes and manages the
/// multi-step process of migrating slots and keys.
async fn run_reshard_orchestrator(
    state: Arc<ServerState>,
    source_id: String,
    dest_id: String,
    slots: Vec<u16>,
) -> Result<(), anyhow::Error> {
    let cluster = state
        .cluster
        .as_ref()
        .ok_or_else(|| anyhow!("Not in cluster mode"))?;

    // --- Step 1: Pre-flight checks and client setup ---
    let source_node = cluster
        .nodes
        .get(&source_id)
        .ok_or_else(|| anyhow!("Source node {} not found", source_id))?
        .value()
        .node_info
        .clone();

    let dest_node = cluster
        .nodes
        .get(&dest_id)
        .ok_or_else(|| anyhow!("Destination node {} not found", dest_id))?
        .value()
        .node_info
        .clone();

    if !source_node.get_flags().contains(NodeFlags::PRIMARY)
        || !dest_node.get_flags().contains(NodeFlags::PRIMARY)
    {
        return Err(anyhow!(
            "Both source and destination must be primary nodes."
        ));
    }

    let source_addr: SocketAddr = source_node.addr.parse()?;
    let dest_addr: SocketAddr = dest_node.addr.parse()?;

    info!(
        "[RESHARD] Connecting to source node {} ({})",
        source_id, source_addr
    );
    let mut source_client = ClusterClient::connect(source_addr).await?;
    info!(
        "[RESHARD] Connecting to destination node {} ({})",
        dest_id, dest_addr
    );
    let mut dest_client = ClusterClient::connect(dest_addr).await?;

    // --- Step 2: Iterate through each slot and migrate it ---
    for slot in slots {
        info!("[RESHARD SLOT {}] Starting process.", slot);
        if !source_node.slots.contains(&slot) {
            warn!(
                "[RESHARD SLOT {}] Slot does not belong to source node {}. Skipping.",
                slot, source_id
            );
            continue;
        }

        // --- Step 2a: Set IMPORTING/MIGRATING state ---
        info!(
            "[RESHARD SLOT {}] Step 1/5: Setting slot to IMPORTING on destination {}.",
            slot, dest_id
        );
        dest_client
            .cluster_setslot(vec![
                "SETSLOT".into(),
                slot.to_string().into(),
                "IMPORTING".into(),
                source_id.clone().into(),
            ])
            .await?;

        info!(
            "[RESHARD SLOT {}] Step 2/5: Setting slot to MIGRATING on source {}.",
            slot, source_id
        );
        source_client
            .cluster_setslot(vec![
                "SETSLOT".into(),
                slot.to_string().into(),
                "MIGRATING".into(),
                dest_id.clone().into(),
            ])
            .await?;

        // --- Step 2b: Migrate all keys in the slot ---
        info!("[RESHARD SLOT {}] Step 3/5: Migrating keys...", slot);
        loop {
            let keys_to_move = source_client.get_keys_in_slot(slot, 10).await?;
            if keys_to_move.is_empty() {
                info!("[RESHARD SLOT {}] All keys have been migrated.", slot);
                break;
            }
            for key in keys_to_move {
                debug!(
                    "[RESHARD SLOT {}] Migrating key: {}",
                    slot,
                    String::from_utf8_lossy(&key)
                );
                let dest_host = dest_node.addr.split(':').next().unwrap().to_string();
                let dest_port = dest_node.addr.split(':').next_back().unwrap().parse()?;

                source_client
                    .migrate_key(dest_host, dest_port, key, 0, 5000)
                    .await?;
            }
        }

        // --- Step 2c: Finalize the slot ownership change across the cluster ---
        info!(
            "[RESHARD SLOT {}] Step 4/5: Broadcasting final ownership to all nodes.",
            slot
        );
        for node_entry in cluster.nodes.iter() {
            let node_info = &node_entry.value().node_info;
            info!("  -> Notifying node {} ({})", node_info.id, &node_info.addr);
            if let Ok(node_addr) = node_info.addr.parse() {
                if let Ok(mut client) = ClusterClient::connect(node_addr).await {
                    let setslot_args = vec![
                        "SETSLOT".into(),
                        slot.to_string().into(),
                        "NODE".into(),
                        dest_id.clone().into(),
                    ];
                    if let Err(e) = client.cluster_setslot(setslot_args).await {
                        warn!(
                            "Failed to notify node {}: {}. Gossip will eventually sync.",
                            &node_info.addr, e
                        );
                    }
                } else {
                    warn!(
                        "Failed to connect to node {} for SETSLOT. Gossip will eventually sync.",
                        &node_info.addr
                    );
                }
            } else {
                warn!(
                    "Could not parse address for node {}: {}",
                    node_info.id, &node_info.addr
                );
            }
        }

        // --- Step 2d: Persist the configuration ---
        info!(
            "[RESHARD SLOT {}] Step 5/5: Saving new cluster configuration.",
            slot
        );
        cluster.save_config().await?;
        info!("[RESHARD SLOT {}] Resharding complete for this slot.", slot);
    }

    Ok(())
}
