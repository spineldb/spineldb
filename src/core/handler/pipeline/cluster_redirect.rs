// src/core/handler/pipeline/cluster_redirect.rs

//! Pipeline step for handling cluster redirections (MOVED/ASK).

use crate::connection::SessionState;
use crate::core::SpinelDBError;
use crate::core::cluster::slot;
use crate::core::state::ServerState;
use bytes::Bytes;
use std::sync::Arc;

/// Checks if a command targeting specific keys should be redirected to another node.
pub async fn check_redirection(
    state: &Arc<ServerState>,
    keys: &[Bytes],
    session: &SessionState,
) -> Result<(), SpinelDBError> {
    let Some(cluster_state) = &state.cluster else {
        return Ok(());
    };
    if keys.is_empty() {
        return Ok(());
    }

    let first_slot = slot::get_slot(&keys[0]);
    if keys.len() > 1 && !keys.iter().all(|k| slot::get_slot(k) == first_slot) {
        return Err(SpinelDBError::CrossSlot);
    }

    let my_id = &cluster_state.my_id;
    let myself = cluster_state.nodes.get(my_id).ok_or_else(|| {
        SpinelDBError::ClusterDown("Could not find self in cluster node map".to_string())
    })?;

    // Handle IMPORTING state: redirect unless the client sent ASKING.
    if let Some(source_node_id) = myself.node_info.importing_slots.get(&first_slot) {
        if !session.is_asking {
            let source_node = cluster_state.nodes.get(source_node_id).ok_or_else(|| {
                SpinelDBError::ClusterDown(format!(
                    "Importing source node {source_node_id} not found"
                ))
            })?;
            return Err(SpinelDBError::Moved {
                slot: first_slot,
                addr: source_node.node_info.addr.clone(),
            });
        }
    }

    // Handle MIGRATING state: send ASK redirect if key doesn't exist locally.
    if let Some(dest_node_id) = myself.node_info.migrating_slots.get(&first_slot) {
        let db = state.get_db(session.current_db_index).unwrap();
        let shard_index = db.get_shard_index(&keys[0]);
        let guard = db.get_shard(shard_index).entries.lock().await;
        if guard.peek(&keys[0]).is_none_or(|e| e.is_expired()) {
            let dest_node = cluster_state.nodes.get(dest_node_id).ok_or_else(|| {
                SpinelDBError::ClusterDown(format!(
                    "Migrating destination node {dest_node_id} not found"
                ))
            })?;
            return Err(SpinelDBError::Ask {
                slot: first_slot,
                addr: dest_node.node_info.addr.clone(),
            });
        }
    }

    // If the client is in the ASKING state, no further redirection checks are needed.
    if session.is_asking {
        return Ok(());
    }

    // Handle standard MOVED redirection if this node is not the slot owner.
    if let Some(owner_node) = cluster_state.get_node_for_slot(first_slot) {
        if owner_node.node_info.id != *my_id {
            return Err(SpinelDBError::Moved {
                slot: first_slot,
                addr: owner_node.node_info.addr.clone(),
            });
        }
    }

    Ok(())
}
