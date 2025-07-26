// src/core/commands/cluster/getkeysinslot.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    slot: u16,
    count: usize,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx.state.cluster.as_ref().unwrap();

    if !cluster.i_own_slot(slot) {
        return Err(SpinelDBError::InvalidState(format!(
            "Slot {slot} is not served by this instance"
        )));
    }

    let keys = ctx.db.get_keys_in_slot(slot, count).await;
    let resp_keys = keys.into_iter().map(RespValue::BulkString).collect();

    Ok((RespValue::Array(resp_keys), WriteOutcome::DidNotWrite))
}
