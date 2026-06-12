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
        // Legacy password authentication using constant-time comparison
        if pass.len() == auth_cmd.password.len()
            && pass
                .as_bytes()
                .iter()
                .zip(auth_cmd.password.as_bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0
        {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::test_helpers::init_server_state;

    fn make_test_state_no_auth() -> Arc<ServerState> {
        init_server_state(Config::default())
    }

    fn make_test_state_with_password(password: &str) -> Arc<ServerState> {
        let config = Config {
            password: Some(password.to_string()),
            ..Default::default()
        };
        init_server_state(config)
    }

    #[tokio::test]
    async fn test_auth_reauthentication() {
        let state = make_test_state_with_password("secret123");
        let mut session = SessionState::new(true, false);

        // First auth
        handle_auth(
            Auth {
                password: "secret123".to_string(),
            },
            &mut session,
            &state,
        )
        .await
        .unwrap();
        assert!(session.is_authenticated);

        // Second auth (re-auth)
        let result = handle_auth(
            Auth {
                password: "secret123".to_string(),
            },
            &mut session,
            &state,
        )
        .await;
        assert!(result.is_ok());

        match result.unwrap() {
            RouteResponse::Single(RespValue::SimpleString(s)) => assert_eq!(s, "OK"),
            _ => panic!("Expected SimpleString OK for re-authentication"),
        }
    }

    #[tokio::test]
    async fn test_auth_no_password_set() {
        let state = make_test_state_no_auth();
        let mut session = SessionState::new(true, false);

        let auth_cmd = Auth {
            password: "test".to_string(),
        };

        let result = handle_auth(auth_cmd, &mut session, &state).await;
        assert!(result.is_ok());
        assert!(!session.is_authenticated);

        match result.unwrap() {
            RouteResponse::Single(RespValue::Error(s)) => {
                assert!(s.contains("no password is set"))
            }
            _ => panic!("Expected Error for no password set"),
        }
    }

    #[tokio::test]
    async fn test_auth_correct_password() {
        let state = make_test_state_with_password("secret123");
        let mut session = SessionState::new(true, false);
        assert!(!session.is_authenticated);

        let auth_cmd = Auth {
            password: "secret123".to_string(),
        };

        let result = handle_auth(auth_cmd, &mut session, &state).await;
        assert!(result.is_ok());
        assert!(session.is_authenticated);

        match result.unwrap() {
            RouteResponse::Single(RespValue::SimpleString(s)) => assert_eq!(s, "OK"),
            _ => panic!("Expected SimpleString OK"),
        }
    }

    #[tokio::test]
    async fn test_auth_wrong_password() {
        let state = make_test_state_with_password("secret123");
        let mut session = SessionState::new(true, false);

        let auth_cmd = Auth {
            password: "wrong".to_string(),
        };

        let result = handle_auth(auth_cmd, &mut session, &state).await;
        assert!(result.is_err());
        assert!(!session.is_authenticated);
        assert!(matches!(
            result.unwrap_err(),
            SpinelDBError::InvalidPassword
        ));
    }
}
