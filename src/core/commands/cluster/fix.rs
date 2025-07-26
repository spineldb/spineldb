// src/core/commands/cluster/fix.rs

use crate::core::cluster::client::ClusterClient;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::state::ServerState;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use anyhow::anyhow;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    info!("CLUSTER FIX command initiated.");
    let state_clone = ctx.state.clone();

    let handle = tokio::spawn(async move { run_fix_orchestrator(state_clone).await });

    let fix_result = handle
        .await
        .map_err(|e| SpinelDBError::Internal(format!("CLUSTER FIX task panicked: {e}")))?;

    match fix_result {
        Ok(log) => {
            let log_string = log.join("\n");
            Ok((
                RespValue::BulkString(log_string.into()),
                // Considered a write as it modifies cluster state on other nodes
                WriteOutcome::Write { keys_modified: 0 },
            ))
        }
        Err(e) => Err(SpinelDBError::Internal(format!("CLUSTER FIX failed: {e}"))),
    }
}

async fn run_fix_orchestrator(state: Arc<ServerState>) -> Result<Vec<String>, anyhow::Error> {
    let cluster = state
        .cluster
        .as_ref()
        .ok_or_else(|| anyhow!("Not in cluster mode"))?;
    let mut fixes_log = Vec::new();
    let all_nodes: Vec<_> = cluster
        .nodes
        .iter()
        .map(|e| e.value().node_info.clone())
        .collect();

    for node in &all_nodes {
        // Fix slots stuck in MIGRATING state
        for (slot, dest_id) in &node.migrating_slots {
            let log_msg = format!(
                "Found stuck MIGRATING slot {slot} on node {}. Attempting to fix.",
                node.id
            );
            info!("[CLUSTER FIX] {}", log_msg);
            fixes_log.push(log_msg);

            let source_addr: SocketAddr = node.addr.parse()?;
            let mut source_client = ClusterClient::connect(source_addr).await?;
            source_client
                .cluster_setslot(vec![
                    "SETSLOT".into(),
                    slot.to_string().into(),
                    "STABLE".into(),
                ])
                .await
                .map_err(|e| anyhow!("Failed to send STABLE to source {}: {}", node.id, e))?;

            if let Some(dest_node_info) = all_nodes.iter().find(|n| n.id == *dest_id) {
                let dest_addr: SocketAddr = dest_node_info.addr.parse()?;
                let mut dest_client = ClusterClient::connect(dest_addr).await?;
                dest_client
                    .cluster_setslot(vec![
                        "SETSLOT".into(),
                        slot.to_string().into(),
                        "STABLE".into(),
                    ])
                    .await
                    .map_err(|e| {
                        anyhow!("Failed to send STABLE to destination {}: {}", dest_id, e)
                    })?;
            }
            fixes_log.push(format!("-> Reverted slot {slot} to STABLE state."));
        }

        // Fix slots stuck in IMPORTING state (for asymmetric cases)
        for (slot, source_id) in &node.importing_slots {
            if let Some(source_node_info) = all_nodes.iter().find(|n| &n.id == source_id) {
                if source_node_info.migrating_slots.contains_key(slot) {
                    continue;
                } // Already handled
            }
            let log_msg = format!(
                "Found stuck IMPORTING slot {slot} on node {}. Attempting to fix.",
                node.id
            );
            info!("[CLUSTER FIX] {}", log_msg);
            fixes_log.push(log_msg);

            let dest_addr: SocketAddr = node.addr.parse()?;
            let mut dest_client = ClusterClient::connect(dest_addr).await?;
            dest_client
                .cluster_setslot(vec![
                    "SETSLOT".into(),
                    slot.to_string().into(),
                    "STABLE".into(),
                ])
                .await
                .map_err(|e| anyhow!("Failed to send STABLE to destination {}: {}", node.id, e))?;
            fixes_log.push(format!(
                "-> Reverted slot {slot} to STABLE state on node {}.",
                node.id
            ));
        }
    }

    cluster.save_config()?;
    Ok(fixes_log)
}
