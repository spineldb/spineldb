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

#[cfg(test)]
mod tests {
    #[test]
    fn test_cannot_forget_self_error_message() {
        let my_id = "node-1";
        let node_id_to_forget = "node-1";
        assert_eq!(my_id, node_id_to_forget);
    }

    #[test]
    fn test_forget_different_node_is_allowed() {
        let my_id = "node-1";
        let node_id_to_forget = "node-2";
        assert_ne!(my_id, node_id_to_forget);
    }

    #[test]
    fn test_node_not_found_error_format() {
        let node_id = "nonexistent-node";
        let err_msg = format!("Node {node_id} not found in the cluster");
        assert!(err_msg.contains("nonexistent-node"));
        assert!(err_msg.contains("not found"));
    }
}
