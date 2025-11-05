// src/core/commands/cluster/reshard.rs

use crate::core::cluster::NodeFlags;
use crate::core::cluster::client::ClusterClient;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::ExecutionContext;
use crate::core::state::ServerState;
use crate::core::{RespValue, SpinelDBError};
use anyhow::anyhow;
use futures::future::join_all;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// --- Constants for resharding configuration ---

/// The number of keys to fetch from the source node in a single batch.
const KEY_BATCH_SIZE: usize = 100;
/// The number of concurrent key migrations to perform.
const CONCURRENT_MIGRATIONS: usize = 16;

/// Executes the CLUSTER RESHARD command by spawning a background orchestrator task.
pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    source_node_id: &str,
    destination_node_id: &str,
    slots: &[u16],
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let state_for_task = ctx.state.clone();
    let source_clone = source_node_id.to_owned();
    let dest_clone = destination_node_id.to_owned();
    let slots_clone = slots.to_vec();

    // Spawn the long-running reshard logic into a separate, critical task.
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

    ctx.state.critical_tasks.lock().await.spawn(task);

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

    // Ensure both source and destination nodes are found and are primaries.
    if !source_node.get_flags().contains(NodeFlags::PRIMARY)
        || !dest_node.get_flags().contains(NodeFlags::PRIMARY)
    {
        return Err(anyhow!(
            "Both source and destination must be primary nodes."
        ));
    }

    let source_addr: SocketAddr = source_node.addr.parse()?;

    // --- Step 2: Establish connection pools ---
    info!("[RESHARD] Building connection pools...");

    // A pool for broadcasting state changes to all nodes except the source.
    let mut broadcast_clients: HashMap<String, ClusterClient> = HashMap::new();
    for node_entry in cluster.nodes.iter() {
        let node_id = node_entry.key();
        if node_id == &source_id {
            continue;
        }

        let node_info = &node_entry.value().node_info;
        if let Ok(addr) = node_info.addr.parse::<SocketAddr>() {
            match ClusterClient::connect(addr).await {
                Ok(client) => {
                    broadcast_clients.insert(node_id.clone(), client);
                    info!("-> Connected to broadcast node {} ({})", node_id, addr);
                }
                Err(e) => warn!("-> FAILED to connect to node {} ({}): {}", node_id, addr, e),
            }
        }
    }

    // A simple connection pool for concurrent migrations from the source node.
    let (client_tx, mut client_rx) = mpsc::channel(CONCURRENT_MIGRATIONS);

    for _ in 0..CONCURRENT_MIGRATIONS {
        let client = ClusterClient::connect(source_addr).await?;
        client_tx.send(client).await?;
    }
    info!(
        "-> Created a pool of {} clients for the source node {}",
        CONCURRENT_MIGRATIONS, source_id
    );

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
        let dest_client = broadcast_clients
            .get_mut(&dest_id)
            .ok_or_else(|| anyhow!("Destination node {} client not found in pool", dest_id))?;

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

        // Temporarily borrow a client from the pool for admin commands.
        let mut source_admin_client = client_rx.recv().await.unwrap();

        info!(
            "[RESHARD SLOT {}] Step 2/5: Setting slot to MIGRATING on source {}.",
            slot, source_id
        );
        source_admin_client
            .cluster_setslot(vec![
                "SETSLOT".into(),
                slot.to_string().into(),
                "MIGRATING".into(),
                dest_id.clone().into(),
            ])
            .await?;

        // --- Step 3b: Migrate all keys in the slot concurrently ---
        info!("[RESHARD SLOT {}] Step 3/5: Migrating keys...", slot);
        loop {
            let keys_to_move = source_admin_client
                .get_keys_in_slot(slot, KEY_BATCH_SIZE)
                .await?;
            if keys_to_move.is_empty() {
                info!("[RESHARD SLOT {}] All keys have been migrated.", slot);
                break;
            }

            // Create a future for each key migration.
            let mut migration_tasks = Vec::new();
            for key in keys_to_move {
                // Acquire a client from the pool for this specific task.
                let mut client = client_rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow!("Client pool was closed unexpectedly"))?;
                let tx = client_tx.clone();
                let dest_node_clone = dest_node.clone();

                let task = async move {
                    debug!(
                        "[RESHARD SLOT {}] Migrating key: {}",
                        slot,
                        String::from_utf8_lossy(&key)
                    );
                    let dest_host = dest_node_clone.addr.split(':').next().unwrap().to_string();
                    let dest_port = dest_node_clone
                        .addr
                        .split(':')
                        .next_back()
                        .unwrap()
                        .parse()?;

                    let result = client.migrate_key(dest_host, dest_port, key, 0, 5000).await;

                    // Return the client to the pool.
                    let _ = tx.send(client).await;
                    result
                };
                migration_tasks.push(task);
            }

            // Wait for all migrations in the current batch to complete.
            let results = join_all(migration_tasks).await;

            // If any migration fails, abort the entire resharding process.
            for result in results {
                if let Err(e) = result {
                    // Return the admin client to the pool before erroring out.
                    let _ = client_tx.send(source_admin_client).await;
                    return Err(anyhow!(
                        "Failed to migrate a key in slot {}: {}. Aborting reshard.",
                        slot,
                        e
                    ));
                }
            }
        }

        // Return the admin client to the source pool.
        let _ = client_tx.send(source_admin_client).await;

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

        // Authoritatively set the new owner for the slot on all involved nodes.
        let mut source_client_for_broadcast = client_rx.recv().await.unwrap();
        source_client_for_broadcast
            .cluster_setslot(setslot_args.clone())
            .await?;
        let _ = client_tx.send(source_client_for_broadcast).await;

        for (node_id, client) in broadcast_clients.iter_mut() {
            info!("  -> Notifying node {}", node_id);
            if let Err(e) = client.cluster_setslot(setslot_args.clone()).await {
                warn!(
                    "Failed to notify node {}: {}. Gossip will eventually sync.",
                    node_id, e
                );
            }
        }

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
