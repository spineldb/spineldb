// src/core/handler/actions/transaction.rs

use crate::connection::SessionState;
use crate::core::database::Db;
use crate::core::handler::command_router::RouteResponse;
use crate::core::handler::transaction_handler::TransactionHandler;
use crate::core::state::ServerState;
use crate::core::{RespValue, SpinelDBError};
use bytes::Bytes;
use std::sync::Arc;

pub fn handle_multi(
    db: &Arc<Db>,
    session: &mut SessionState,
    state: Arc<ServerState>,
    session_id: u64,
) -> Result<RouteResponse, SpinelDBError> {
    TransactionHandler::new(state, db, session_id, session.authenticated_user.clone())
        .handle_multi()?;
    session.is_in_transaction = true;
    Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())))
}

pub async fn handle_exec(
    db: &Arc<Db>,
    session: &mut SessionState,
    state: Arc<ServerState>,
    session_id: u64,
) -> Result<RouteResponse, SpinelDBError> {
    let mut handler =
        TransactionHandler::new(state, db, session_id, session.authenticated_user.clone());
    let response = handler.handle_exec().await?;
    session.is_in_transaction = false;
    Ok(RouteResponse::Single(response))
}

pub fn handle_discard(
    db: &Arc<Db>,
    session: &mut SessionState,
    state: Arc<ServerState>,
    session_id: u64,
) -> Result<RouteResponse, SpinelDBError> {
    TransactionHandler::new(state, db, session_id, session.authenticated_user.clone())
        .handle_discard()
        .map(RouteResponse::Single)
}

pub async fn handle_watch(
    keys: Vec<Bytes>,
    db: &Arc<Db>,
    state: Arc<ServerState>,
    session_id: u64,
    session: &mut SessionState,
) -> Result<RouteResponse, SpinelDBError> {
    TransactionHandler::new(state, db, session_id, session.authenticated_user.clone())
        .handle_watch(keys)
        .await
        .map(RouteResponse::Single)
}

pub fn handle_unwatch(db: &Arc<Db>, session_id: u64) -> Result<RouteResponse, SpinelDBError> {
    if let Some(mut tx_state) = db.tx_states.get_mut(&session_id) {
        tx_state.watched_keys.clear();
    }
    Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())))
}
