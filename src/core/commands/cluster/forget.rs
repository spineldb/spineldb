// src/core/commands/cluster/forget.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use tracing::info;

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    node_id_to_forget: &str,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx
        .state
        .cluster
        .as_ref()
        .expect("CLUSTER FORGET must be run in cluster mode");

    // A node cannot forget itself.
    if node_id_to_forget == cluster.my_id {
        return Err(SpinelDBError::InvalidState(
            "Cannot forget myself".to_string(),
        ));
    }

    // Attempt to remove the node from the cluster state.
    if cluster.nodes.remove(node_id_to_forget).is_some() {
        info!(
            "Node {} has been removed from the cluster configuration.",
            node_id_to_forget
        );

        // Also, remove any PFAIL reports this node might have made about others.
        for mut entry in cluster.nodes.iter_mut() {
            entry.value_mut().pfail_reports.remove(node_id_to_forget);
        }

        // Persist the change to the configuration file.
        cluster.save_config().await?;

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::DidNotWrite, // Config change, not data change
        ))
    } else {
        // The node was not found in the cluster.
        Err(SpinelDBError::InvalidState(format!(
            "Node {node_id_to_forget} not found in the cluster"
        )))
    }
}
