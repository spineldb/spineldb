// src/core/handler/pipeline/acl_check.rs

//! Pipeline step for enforcing Access Control List (ACL) rules.

use crate::connection::SessionState;
use crate::core::commands::command_trait::CommandExt;
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::{Command, SpinelDBError};
use bytes::Bytes;
use std::sync::Arc;

/// Checks and enforces ACL permissions for the current command.
pub async fn check_permissions(
    state: &Arc<ServerState>,
    session: &SessionState,
    command: &Command,
    raw_args: &[RespFrame],
    keys_bytes: &[Bytes],
) -> Result<(), SpinelDBError> {
    // Skip ACL check if not enabled.
    if !state.acl_config.read().await.enabled {
        return Ok(());
    }

    // AUTH is a special case that must be allowed even for unauthenticated users.
    if session.authenticated_user.is_none() && command.name() != "auth" {
        return Err(SpinelDBError::NoPermission);
    }

    let keys_as_strings: Vec<String> = keys_bytes
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();

    // Extract Pub/Sub channels for specific commands that operate on them.
    let pubsub_channels: Vec<String> = match command {
        Command::Subscribe(c) => c
            .channels
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect(),
        Command::PSubscribe(c) => c
            .patterns
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect(),
        Command::Publish(c) => vec![String::from_utf8_lossy(&c.channel).into_owned()],
        _ => vec![],
    };

    if !state.acl_enforcer.read().await.check_permission(
        session.authenticated_user.as_deref(),
        raw_args,
        command.name(),
        command.get_flags(),
        &keys_as_strings,
        &pubsub_channels,
    ) {
        return Err(SpinelDBError::NoPermission);
    }

    Ok(())
}
