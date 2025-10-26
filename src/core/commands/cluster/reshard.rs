// src/core/commands/cluster/reshard.rs

use crate::core::cluster::NodeFlags;
use crate::core::cluster::client::ClusterClient;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::state::ServerState;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use anyhow::anyhow;
use std::collections::HashMap;
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
    let state_for_task = ctx.state.clone();
    let state_for_spawning = ctx.state.clone();

    let source_clone = source_node_id.to_owned();
    let dest_clone = destination_node_id.to_owned();
    let slots_clone = slots.to_vec();

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

    state_for_spawning.critical_tasks.lock().await.spawn(task);

    Ok((
        RespValue::SimpleString("OK".into()),
        WriteOutcome::DidNotWrite,
    ))
}

/// The main resharding orchestrator. It connects to all nodes and manages the
/// multi-step process of migrating slots and keys using a connection pool.
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

    // --- Step 1: Pre-flight checks ---
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

    // --- Step 2: Establish a persistent connection pool to all cluster nodes ---
    info!("[RESHARD] Building connection pool to all cluster nodes...");
    let mut client_pool: HashMap<String, ClusterClient> = HashMap::new();

    for node_entry in cluster.nodes.iter() {
        let node_id = node_entry.key();
        let node_info = &node_entry.value().node_info;
        if let Ok(addr) = node_info.addr.parse::<SocketAddr>() {
            match ClusterClient::connect(addr).await {
                Ok(client) => {
                    client_pool.insert(node_id.clone(), client);
                    info!("-> Connected to node {} ({})", node_id, addr);
                }
                Err(e) => {
                    warn!(
                        "-> FAILED to connect to node {} ({}): {}. It may not receive resharding updates.",
                        node_id, addr, e
                    );
                }
            }
        }
    }

    // --- PERBAIKAN: Ambil (pindahkan) client dari pool, jangan meminjamnya ---
    // Remove the source and destination clients from the pool to get mutable ownership.
    // The rest of the pool will be used for broadcasting.
    let mut source_client = client_pool.remove(&source_id).ok_or_else(|| {
        anyhow!(
            "Failed to establish connection to source node {}",
            source_id
        )
    })?;
    let mut dest_client = client_pool.remove(&dest_id).ok_or_else(|| {
        anyhow!(
            "Failed to establish connection to destination node {}",
            dest_id
        )
    })?;

    // --- Step 3: Iterate through each slot and migrate it ---
    for slot in slots {
        info!("[RESHARD SLOT {}] Starting process.", slot);
        if !source_node.slots.contains(&slot) {
            warn!(
                "[RESHARD SLOT {}] Slot does not belong to source node {}. Skipping.",
                slot, source_id
            );
            continue;
        }

        // --- Step 3a: Set IMPORTING/MIGRATING state ---
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

        // --- Step 3b: Migrate all keys in the slot ---
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

        // --- Step 3c: Finalize the slot ownership change across the cluster ---
        info!(
            "[RESHARD SLOT {}] Step 4/5: Broadcasting final ownership to all nodes.",
            slot
        );
        let setslot_args = vec![
            "SETSLOT".into(),
            slot.to_string().into(),
            "NODE".into(),
            dest_id.clone().into(),
        ];

        // Broadcast to the remaining clients in the pool
        for (node_id, client) in client_pool.iter_mut() {
            info!("  -> Notifying node {}", node_id);
            if let Err(e) = client.cluster_setslot(setslot_args.clone()).await {
                warn!(
                    "Failed to notify node {}: {}. Gossip will eventually sync.",
                    node_id, e
                );
            }
        }
        // Also send to source and destination
        source_client.cluster_setslot(setslot_args.clone()).await?;
        dest_client.cluster_setslot(setslot_args.clone()).await?;

        // --- Step 3d: Persist the configuration ---
        info!(
            "[RESHARD SLOT {}] Step 5/5: Saving new cluster configuration.",
            slot
        );
        cluster.save_config().await?;
        info!("[RESHARD SLOT {}] Resharding complete for this slot.", slot);
    }

    Ok(())
}
