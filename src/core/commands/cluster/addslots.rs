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
