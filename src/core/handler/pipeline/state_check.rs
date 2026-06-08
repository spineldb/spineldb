// src/core/handler/pipeline/state_check.rs

//! Pipeline step for checking global server state (read-only, OOM, etc.).

use crate::core::commands::command_trait::{CommandExt, CommandFlags};
use crate::core::state::ServerState;
use crate::core::{Command, SpinelDBError};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Checks global server conditions before executing a command.
pub async fn check_server_state(
    state: &Arc<ServerState>,
    command: &Command,
) -> Result<(), SpinelDBError> {
    let flags = command.get_flags();
    let is_write = flags.contains(CommandFlags::WRITE);

    // Check for write commands in read-only mode.
    if is_write {
        // Check for administrative read-only mode.
        if state.is_read_only.load(Ordering::SeqCst) {
            let reason = if state.event_bus.is_closed() {
                "due to a critical persistence error"
            } else {
                ""
            };
            return Err(SpinelDBError::ReadOnly(format!(
                "Server is in read-only mode {reason}"
            )));
        }

        // Check for self-fencing read-only mode due to quorum loss.
        if state.is_read_only_due_to_quorum_loss.load(Ordering::SeqCst) {
            return Err(SpinelDBError::ClusterDown(
                "The master is in a read-only state due to losing contact with the cluster majority. Writes are not accepted.".into()
            ));
        }

        // Check min-replicas policy for data safety.
        state.replication.check_min_replicas_policy(state).await?;
    }

    let config = state.config.lock().await;

    // Check for write commands on a replica instance.
    if is_write
        && matches!(
            config.replication,
            crate::config::ReplicationConfig::Replica { .. }
        )
    {
        return Err(SpinelDBError::ReadOnly(
            "You can't write against a read only replica.".into(),
        ));
    }

    // Check for OOM condition for commands that can allocate significant memory.
    if flags.contains(CommandFlags::DENY_OOM)
        && let Some(maxmemory) = config.maxmemory
    {
        let total_memory: usize = state.dbs.iter().map(|db| db.get_current_memory()).sum();
        if total_memory >= maxmemory {
            return Err(SpinelDBError::MaxMemoryReached);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::core::commands::string::Set;
    use crate::test_helpers::init_server_state;
    use bytes::Bytes;

    fn make_test_state_read_only() -> Arc<ServerState> {
        let state = init_server_state(Config::default());
        state.set_read_only(true, "test");
        state
    }

    fn make_test_state_normal() -> Arc<ServerState> {
        init_server_state(Config::default())
    }

    #[tokio::test]
    async fn test_read_only_blocks_write_command() {
        let state = make_test_state_read_only();
        let cmd = Command::Set(Set {
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(b"val"),
            ..Set::default()
        });
        let result = check_server_state(&state, &cmd).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            SpinelDBError::ReadOnly(_) => {}
            other => panic!("Expected ReadOnly error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_read_only_allows_read_command() {
        let state = make_test_state_read_only();
        let cmd = Command::Get(crate::core::commands::string::Get {
            key: Bytes::from_static(b"test"),
        });
        let result = check_server_state(&state, &cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_normal_state_allows_write() {
        let state = make_test_state_normal();
        let cmd = Command::Set(Set {
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(b"val"),
            ..Set::default()
        });
        let result = check_server_state(&state, &cmd).await;
        assert!(result.is_ok());
    }
}
