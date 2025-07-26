// src/core/commands/cluster/setslot.rs

use super::SetSlotSubcommand;
use crate::core::cluster::slot::NUM_SLOTS;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    slot: u16,
    subcmd: &SetSlotSubcommand,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx.state.cluster.as_ref().unwrap();

    if slot >= NUM_SLOTS as u16 {
        return Err(SpinelDBError::InvalidState("Invalid slot".to_string()));
    }

    match subcmd {
        SetSlotSubcommand::Migrating(dest_node_id) => {
            let mut myself = cluster.nodes.get_mut(&cluster.my_id).unwrap();
            if !myself.node_info.slots.contains(&slot) {
                return Err(SpinelDBError::InvalidState(
                    "Cannot MIGRATE a slot I don't own".to_string(),
                ));
            }
            myself
                .node_info
                .migrating_slots
                .insert(slot, dest_node_id.clone());
        }
        SetSlotSubcommand::Importing(src_node_id) => {
            let mut myself = cluster.nodes.get_mut(&cluster.my_id).unwrap();
            myself
                .node_info
                .importing_slots
                .insert(slot, src_node_id.clone());
        }
        SetSlotSubcommand::Node(new_owner_id) => {
            // Clear migration state from all nodes for this slot
            for mut node in cluster.nodes.iter_mut() {
                node.node_info.migrating_slots.remove(&slot);
                node.node_info.importing_slots.remove(&slot);
            }

            // Remove slot from old owner
            if let Some(id) = { cluster.slots_map[slot as usize].read().clone() } {
                if let Some(mut old_owner) = cluster.nodes.get_mut(&id) {
                    old_owner.node_info.slots.remove(&slot);
                }
            }

            // Assign slot to new owner
            if let Some(mut new_owner) = cluster.nodes.get_mut(new_owner_id) {
                new_owner.node_info.slots.insert(slot);
                *cluster.slots_map[slot as usize].write() = Some(new_owner_id.clone());
            } else {
                return Err(SpinelDBError::InvalidState(format!(
                    "Node {new_owner_id} not found"
                )));
            }
        }
        SetSlotSubcommand::Stable => {
            let mut myself = cluster.nodes.get_mut(&cluster.my_id).unwrap();
            myself.node_info.migrating_slots.remove(&slot);
            myself.node_info.importing_slots.remove(&slot);
        }
    }

    cluster.save_config()?;

    Ok((
        RespValue::SimpleString("OK".into()),
        WriteOutcome::DidNotWrite,
    ))
}
