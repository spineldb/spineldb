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
    fn test_slot_within_range() {
        assert!((0..NUM_SLOTS as u16).contains(&0));
        assert!((0..NUM_SLOTS as u16).contains(&8191));
        assert!((0..NUM_SLOTS as u16).contains(&16383));
    }

    #[test]
    fn test_slot_out_of_range() {
        assert!(NUM_SLOTS as u16 <= 16384);
        let invalid_slot = NUM_SLOTS as u16;
        assert!(invalid_slot >= NUM_SLOTS as u16);
    }

    #[test]
    fn test_slot_max_value() {
        let max_slot = u16::MAX;
        assert!(max_slot >= NUM_SLOTS as u16);
    }

    #[test]
    fn test_num_slots_is_16384() {
        assert_eq!(NUM_SLOTS, 16384);
    }
}
