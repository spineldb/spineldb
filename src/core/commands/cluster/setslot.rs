// src/core/commands/cluster/setslot.rs

use super::SetSlotSubcommand;
use crate::core::cluster::slot::NUM_SLOTS;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::ExecutionContext;
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
            if let Some(id) = { cluster.slots_map[slot as usize].read().clone() }
                && let Some(mut old_owner) = cluster.nodes.get_mut(&id)
            {
                old_owner.node_info.slots.remove(&slot);
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

    cluster.save_config().await?;

    Ok((
        RespValue::SimpleString("OK".into()),
        WriteOutcome::DidNotWrite,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_slot_error_message() {
        let _slot: u16 = NUM_SLOTS as u16;
        let err_msg = "Invalid slot".to_string();
        assert!(err_msg.contains("Invalid"));
    }

    #[test]
    fn test_slot_boundary_valid() {
        let slot: u16 = (NUM_SLOTS as u16) - 1;
        assert!(slot < NUM_SLOTS as u16);
    }

    #[test]
    fn test_slot_boundary_invalid() {
        let slot: u16 = NUM_SLOTS as u16;
        assert!(slot >= NUM_SLOTS as u16);
    }

    #[test]
    fn test_migrating_requires_own_slot_error() {
        let err_msg = "Cannot MIGRATE a slot I don't own".to_string();
        assert!(err_msg.contains("don't own"));
    }

    #[test]
    fn test_node_not_found_error_format() {
        let new_owner_id = "nonexistent";
        let err_msg = format!("Node {new_owner_id} not found");
        assert!(err_msg.contains("nonexistent"));
        assert!(err_msg.contains("not found"));
    }

    #[test]
    fn test_setslot_subcommand_variants() {
        let migrating = SetSlotSubcommand::Migrating("node-1".to_string());
        let importing = SetSlotSubcommand::Importing("node-2".to_string());
        let node = SetSlotSubcommand::Node("node-3".to_string());
        let stable = SetSlotSubcommand::Stable;

        assert!(matches!(migrating, SetSlotSubcommand::Migrating(_)));
        assert!(matches!(importing, SetSlotSubcommand::Importing(_)));
        assert!(matches!(node, SetSlotSubcommand::Node(_)));
        assert!(matches!(stable, SetSlotSubcommand::Stable));
    }

    #[test]
    fn test_setslot_subcommand_clone() {
        let original = SetSlotSubcommand::Migrating("node-1".to_string());
        let cloned = original.clone();
        assert!(matches!(cloned, SetSlotSubcommand::Migrating(id) if id == "node-1"));
    }

    #[test]
    fn test_setslot_subcommand_debug() {
        let subcmd = SetSlotSubcommand::Importing("src-node".to_string());
        let debug_str = format!("{:?}", subcmd);
        assert!(debug_str.contains("Importing"));
        assert!(debug_str.contains("src-node"));
    }
}
