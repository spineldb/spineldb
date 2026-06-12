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
    use std::collections::HashMap;

    struct FakeCluster {
        my_id: String,
        nodes: HashMap<String, ()>,
    }

    impl FakeCluster {
        fn can_forget(&self, node_id: &str) -> Result<(), &'static str> {
            if node_id == self.my_id {
                return Err("Cannot forget myself");
            }
            if !self.nodes.contains_key(node_id) {
                return Err("Node not found in the cluster");
            }
            Ok(())
        }

        fn do_forget(&mut self, node_id: &str) -> Result<(), &'static str> {
            self.can_forget(node_id)?;
            self.nodes.remove(node_id);
            Ok(())
        }
    }

    #[test]
    fn test_self_forget_is_rejected() {
        let cluster = FakeCluster {
            my_id: "node-1".to_string(),
            nodes: [("node-1".to_string(), ()), ("node-2".to_string(), ())].into(),
        };
        assert_eq!(cluster.can_forget("node-1"), Err("Cannot forget myself"));
    }

    #[test]
    fn test_forget_different_node_succeeds() {
        let mut cluster = FakeCluster {
            my_id: "node-1".to_string(),
            nodes: [("node-1".to_string(), ()), ("node-2".to_string(), ())].into(),
        };
        assert!(cluster.do_forget("node-2").is_ok());
        assert!(!cluster.nodes.contains_key("node-2"));
    }

    #[test]
    fn test_forget_nonexistent_node_fails() {
        let mut cluster = FakeCluster {
            my_id: "node-1".to_string(),
            nodes: [("node-1".to_string(), ())].into(),
        };
        assert_eq!(
            cluster.do_forget("node-999"),
            Err("Node not found in the cluster")
        );
    }

    #[test]
    fn test_forget_removes_only_target_node() {
        let mut cluster = FakeCluster {
            my_id: "node-1".to_string(),
            nodes: [
                ("node-1".to_string(), ()),
                ("node-2".to_string(), ()),
                ("node-3".to_string(), ()),
            ]
            .into(),
        };
        cluster.do_forget("node-2").unwrap();
        assert!(cluster.nodes.contains_key("node-1"));
        assert!(!cluster.nodes.contains_key("node-2"));
        assert!(cluster.nodes.contains_key("node-3"));
    }

    #[test]
    fn test_self_check_before_forget() {
        let cluster = FakeCluster {
            my_id: "node-1".to_string(),
            nodes: [("node-1".to_string(), ()), ("node-2".to_string(), ())].into(),
        };
        assert!(cluster.can_forget("node-2").is_ok());
        assert!(cluster.can_forget("node-1").is_err());
    }
}
