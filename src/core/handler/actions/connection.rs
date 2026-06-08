// src/core/handler/actions/connection.rs

use crate::connection::SessionState;
use crate::core::commands::generic::{Replconf, Select};
use crate::core::handler::command_router::RouteResponse;
use crate::core::state::ServerState;
use crate::core::{RespValue, SpinelDBError};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

pub async fn handle_select(
    cmd: Select,
    session: &mut SessionState,
    state: &Arc<ServerState>,
    session_id: u64,
) -> Result<RouteResponse, SpinelDBError> {
    let max_dbs = state.config.lock().await.databases;
    if cmd.db_index >= max_dbs {
        return Ok(RouteResponse::Single(RespValue::Error(
            "ERR DB index out of range".to_string(),
        )));
    }
    session.current_db_index = cmd.db_index;
    if let Some(client_info) = state.clients.get(&session_id) {
        client_info.value().0.lock().await.db_index = cmd.db_index;
    }
    Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())))
}

pub async fn handle_replconf(
    cmd: &Replconf,
    state: &Arc<ServerState>,
    addr: &SocketAddr,
) -> Result<RouteResponse, SpinelDBError> {
    if cmd
        .args
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case("ack"))
        && let Some(offset_str) = cmd.args.get(1)
        && let Ok(offset) = offset_str.parse::<u64>()
        && let Some(mut replica_state) = state.replica_states.get_mut(addr)
    {
        replica_state.value_mut().ack_offset = offset;
        replica_state.value_mut().last_ack_time = Instant::now();
    }
    Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::core::state::{ReplicaStateInfo, ReplicaSyncState};
    use crate::test_helpers::init_server_state;

    fn make_test_state() -> Arc<ServerState> {
        init_server_state(Config::default())
    }

    #[tokio::test]
    async fn test_handle_select_valid_db() {
        let state = make_test_state();
        let mut session = SessionState::new(false, false);
        let session_id = 1u64;
        let cmd = Select { db_index: 5 };

        let result = handle_select(cmd, &mut session, &state, session_id).await;
        assert!(result.is_ok());
        assert_eq!(session.current_db_index, 5);

        match result.unwrap() {
            RouteResponse::Single(RespValue::SimpleString(s)) => assert_eq!(s, "OK"),
            _ => panic!("Expected SimpleString OK"),
        }
    }

    #[tokio::test]
    async fn test_handle_select_db_zero() {
        let state = make_test_state();
        let mut session = SessionState::new(false, false);
        session.current_db_index = 3;
        let session_id = 1u64;
        let cmd = Select { db_index: 0 };

        let result = handle_select(cmd, &mut session, &state, session_id).await;
        assert!(result.is_ok());
        assert_eq!(session.current_db_index, 0);
    }

    #[tokio::test]
    async fn test_handle_select_out_of_range() {
        let state = make_test_state();
        let mut session = SessionState::new(false, false);
        let session_id = 1u64;
        let cmd = Select { db_index: 100 };

        let result = handle_select(cmd, &mut session, &state, session_id).await;
        assert!(result.is_ok());

        match result.unwrap() {
            RouteResponse::Single(RespValue::Error(s)) => {
                assert!(s.contains("DB index out of range"))
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[tokio::test]
    async fn test_handle_replconf_ok() {
        let state = make_test_state();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let cmd = Replconf {
            args: vec!["ACK".to_string(), "1000".to_string()],
        };

        let result = handle_replconf(&cmd, &state, &addr).await;
        assert!(result.is_ok());

        match result.unwrap() {
            RouteResponse::Single(RespValue::SimpleString(s)) => assert_eq!(s, "OK"),
            _ => panic!("Expected SimpleString OK"),
        }
    }

    #[tokio::test]
    async fn test_handle_replconf_updates_replica_state() {
        let state = make_test_state();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        state.replica_states.insert(
            addr,
            ReplicaStateInfo {
                sync_state: ReplicaSyncState::Online,
                ack_offset: 0,
                last_ack_time: Instant::now(),
            },
        );

        let cmd = Replconf {
            args: vec!["ACK".to_string(), "5000".to_string()],
        };

        let _ = handle_replconf(&cmd, &state, &addr).await;

        let replica = state.replica_states.get(&addr).unwrap();
        assert_eq!(replica.ack_offset, 5000);
    }

    #[tokio::test]
    async fn test_handle_replconf_non_ack_ignored() {
        let state = make_test_state();
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        state.replica_states.insert(
            addr,
            ReplicaStateInfo {
                sync_state: ReplicaSyncState::Online,
                ack_offset: 0,
                last_ack_time: Instant::now(),
            },
        );

        let cmd = Replconf {
            args: vec!["GETACK".to_string(), "1000".to_string()],
        };

        let _ = handle_replconf(&cmd, &state, &addr).await;

        let replica = state.replica_states.get(&addr).unwrap();
        assert_eq!(replica.ack_offset, 0);
    }
}
