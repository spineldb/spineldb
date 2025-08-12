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
