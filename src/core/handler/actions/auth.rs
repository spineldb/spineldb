// src/core/handler/actions/auth.rs

use crate::connection::SessionState;
use crate::core::commands::generic::Auth;
use crate::core::handler::command_router::RouteResponse;
use crate::core::state::ServerState;
use crate::core::{RespValue, SpinelDBError};
use argon2::{self, Argon2, PasswordHash, PasswordVerifier};
use std::sync::Arc;

/// Handles the logic for the AUTH command, supporting both legacy password and ACL-based authentication.
pub async fn handle_auth(
    auth_cmd: Auth,
    session: &mut SessionState,
    state: &Arc<ServerState>,
) -> Result<RouteResponse, SpinelDBError> {
    if session.is_authenticated {
        return Ok(RouteResponse::Single(RespValue::Error(
            "ERR user is already authenticated".to_string(),
        )));
    }

    let config = state.config.lock().await;
    let acl_config = state.acl_config.read().await;

    if acl_config.enabled {
        // ACL authentication using Argon2
        for user in &acl_config.users {
            // Attempt to parse the stored hash.
            if let Ok(parsed_hash) = PasswordHash::new(&user.password_hash) {
                // Verify the provided password against the stored hash.
                if Argon2::default()
                    .verify_password(auth_cmd.password.as_bytes(), &parsed_hash)
                    .is_ok()
                {
                    session.is_authenticated = true;
                    session.authenticated_user = Some(user.clone().into());
                    return Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())));
                }
            }
        }
        // Add a delay on failure to mitigate timing attacks.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        Err(SpinelDBError::InvalidPassword)
    } else if let Some(pass) = &config.password {
        // Legacy password authentication
        if *pass == auth_cmd.password {
            session.is_authenticated = true;
            Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())))
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Err(SpinelDBError::InvalidPassword)
        }
    } else {
        Ok(RouteResponse::Single(RespValue::Error(
            "ERR Client sent AUTH, but no password is set".to_string(),
        )))
    }
}
