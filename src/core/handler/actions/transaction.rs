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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::core::database::Db;
    use crate::core::state::ServerState;
    use crate::test_helpers::init_server_state;

    fn make_test_db() -> Arc<Db> {
        Arc::new(Db::new())
    }

    fn make_test_state() -> Arc<ServerState> {
        init_server_state(Config::default())
    }

    #[test]
    fn test_handle_unwatch_clears_watched_keys() {
        let db = make_test_db();
        let session_id = 1u64;

        {
            let mut tx_state = db.tx_states.entry(session_id).or_default();
            tx_state
                .watched_keys
                .insert(Bytes::from_static(b"key1"), Some(1));
            tx_state
                .watched_keys
                .insert(Bytes::from_static(b"key2"), Some(2));
        }

        let result = handle_unwatch(&db, session_id);
        assert!(result.is_ok());
        match result.unwrap() {
            RouteResponse::Single(RespValue::SimpleString(s)) => assert_eq!(s, "OK"),
            _ => panic!("Expected SimpleString OK"),
        }

        let tx_state = db.tx_states.get(&session_id).unwrap();
        assert!(tx_state.watched_keys.is_empty());
    }

    #[test]
    fn test_handle_unwatch_no_tx_state() {
        let db = make_test_db();
        let session_id = 999u64;
        let result = handle_unwatch(&db, session_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_multi_starts_transaction() {
        let db = make_test_db();
        let state = make_test_state();
        let mut session = SessionState::new(false, false);
        let session_id = 1u64;

        let result = handle_multi(&db, &mut session, state, session_id);
        assert!(result.is_ok());
        assert!(session.is_in_transaction);

        match result.unwrap() {
            RouteResponse::Single(RespValue::SimpleString(s)) => assert_eq!(s, "OK"),
            _ => panic!("Expected SimpleString OK"),
        }
    }

    #[test]
    fn test_handle_discard_aborts_transaction() {
        let db = make_test_db();
        let state = make_test_state();
        let mut session = SessionState::new(false, false);
        session.is_in_transaction = true;
        let session_id = 1u64;

        db.start_transaction(session_id);

        let result = handle_discard(&db, &mut session, state, session_id);
        assert!(result.is_ok());
        // Note: handle_discard only cleans up DB state, not session state.
        // The router is responsible for updating session.is_in_transaction.
    }
}
