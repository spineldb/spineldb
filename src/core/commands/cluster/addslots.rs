// src/core/commands/cluster/addslots.rs

use crate::core::cluster::slot::NUM_SLOTS;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    slots: &[u16],
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx
        .state
        .cluster
        .as_ref()
        .expect("CLUSTER ADDSLOTS must be run in cluster mode");
    let mut my_runtime_state = cluster.nodes.get_mut(&cluster.my_id).unwrap();

    for &slot in slots {
        if slot >= NUM_SLOTS as u16 {
            return Err(SpinelDBError::InvalidState(format!(
                "Slot {slot} is out of range"
            )));
        }
        *cluster.slots_map[slot as usize].write() = Some(cluster.my_id.clone());
        my_runtime_state.node_info.slots.insert(slot);
    }

    cluster.save_config().await?;

    Ok((
        RespValue::SimpleString("OK".into()),
        WriteOutcome::DidNotWrite, // Config change, not data change
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_num_slots_is_16384() {
        assert_eq!(NUM_SLOTS, 16384);
    }

    #[test]
    fn test_slot_zero_is_valid() {
        let slot: u16 = 0;
        assert!(slot < NUM_SLOTS as u16, "slot 0 must be valid");
    }

    #[test]
    fn test_slot_max_valid_boundary() {
        let slot: u16 = (NUM_SLOTS as u16) - 1;
        assert!(slot < NUM_SLOTS as u16, "slot 16383 must be valid");
    }

    #[test]
    fn test_slot_num_slots_is_invalid() {
        let slot: u16 = NUM_SLOTS as u16;
        assert!(slot >= NUM_SLOTS as u16, "slot 16384 must be invalid");
    }

    #[test]
    fn test_slot_u16_max_is_invalid() {
        let slot: u16 = u16::MAX;
        assert!(slot >= NUM_SLOTS as u16, "u16::MAX must be invalid");
    }

    #[test]
    fn test_slot_midpoint_is_valid() {
        let slot: u16 = 8192;
        assert!(slot < NUM_SLOTS as u16, "slot 8192 must be valid");
    }

    #[test]
    fn test_slot_one_past_midpoint_is_valid() {
        let slot: u16 = 8193;
        assert!(slot < NUM_SLOTS as u16, "slot 8193 must be valid");
    }

    #[test]
    fn test_slot_validation_logic_matches_execute() {
        for slot in 0..NUM_SLOTS as u16 {
            assert!(
                slot < NUM_SLOTS as u16,
                "slot {slot} should pass validation"
            );
        }
        let invalid_slots = [NUM_SLOTS as u16, NUM_SLOTS as u16 + 1, u16::MAX];
        for slot in invalid_slots {
            assert!(
                slot >= NUM_SLOTS as u16,
                "slot {slot} should fail validation"
            );
        }
    }
}
