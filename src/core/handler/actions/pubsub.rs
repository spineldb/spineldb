// src/core/handler/actions/pubsub.rs

use crate::connection::{SessionState, SubscriptionReceiver};
use crate::core::handler::command_router::RouteResponse;
use crate::core::state::ServerState;
use crate::core::storage::db::Db;
use crate::core::{RespValue, SpinelDBError};
use bytes::Bytes;
use std::sync::Arc;

pub fn handle_subscribe(
    channels: Vec<Bytes>,
    session: &mut SessionState,
    state: &Arc<ServerState>,
    db: &Arc<Db>,
    session_id: u64,
) -> Result<RouteResponse, SpinelDBError> {
    if session.is_in_transaction {
        db.discard_transaction(session_id)?;
        session.is_in_transaction = false;
    }
    if channels.is_empty() {
        return Ok(RouteResponse::NoOp);
    }
    session.is_subscribed = true;
    let mut responses = Vec::with_capacity(channels.len());
    for name in channels {
        if session.subscribed_channels.insert(name.clone()) {
            let rx = state.pubsub.subscribe(&name);
            session
                .pubsub_receivers
                .push(SubscriptionReceiver::Channel(name.clone(), rx));
        }
        let total_subs = session.subscribed_channels.len() + session.subscribed_patterns.len();
        responses.push(RespValue::Array(vec![
            RespValue::BulkString("subscribe".into()),
            RespValue::BulkString(name),
            RespValue::Integer(total_subs as i64),
        ]));
    }
    Ok(RouteResponse::Multiple(responses))
}

pub fn handle_psubscribe(
    patterns: Vec<Bytes>,
    session: &mut SessionState,
    state: &Arc<ServerState>,
    db: &Arc<Db>,
    session_id: u64,
) -> Result<RouteResponse, SpinelDBError> {
    if session.is_in_transaction {
        db.discard_transaction(session_id)?;
        session.is_in_transaction = false;
    }
    if patterns.is_empty() {
        return Ok(RouteResponse::NoOp);
    }
    session.is_pattern_subscribed = true;
    let mut responses = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        if session.subscribed_patterns.insert(pattern.clone()) {
            let rx = state.pubsub.subscribe_pattern(&pattern);
            session
                .pubsub_receivers
                .push(SubscriptionReceiver::Pattern(pattern.clone(), rx));
        }
        let total_subs = session.subscribed_channels.len() + session.subscribed_patterns.len();
        responses.push(RespValue::Array(vec![
            RespValue::BulkString("psubscribe".into()),
            RespValue::BulkString(pattern),
            RespValue::Integer(total_subs as i64),
        ]));
    }
    Ok(RouteResponse::Multiple(responses))
}

pub fn handle_unsubscribe(
    channels: Vec<Bytes>,
    session: &mut SessionState,
) -> Result<RouteResponse, SpinelDBError> {
    let to_process = if channels.is_empty() {
        std::mem::take(&mut session.subscribed_channels)
            .into_iter()
            .collect()
    } else {
        channels
    };
    let mut responses = Vec::new();
    if to_process.is_empty() && session.subscribed_channels.is_empty() {
        responses.push(RespValue::Array(vec![
            RespValue::BulkString("unsubscribe".into()),
            RespValue::Null,
            RespValue::Integer(session.subscribed_patterns.len() as i64),
        ]));
    } else {
        for name in &to_process {
            if session.subscribed_channels.remove(name) {
                let total_subs =
                    session.subscribed_channels.len() + session.subscribed_patterns.len();
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString("unsubscribe".into()),
                    RespValue::BulkString(name.clone()),
                    RespValue::Integer(total_subs as i64),
                ]));
            }
        }
    }
    session.pubsub_receivers.retain(|r| match r {
        SubscriptionReceiver::Channel(c, _) => session.subscribed_channels.contains(c),
        _ => true,
    });
    if session.subscribed_channels.is_empty() {
        session.is_subscribed = false;
    }
    Ok(RouteResponse::Multiple(responses))
}

pub fn handle_punsubscribe(
    patterns: Vec<Bytes>,
    session: &mut SessionState,
) -> Result<RouteResponse, SpinelDBError> {
    let to_process = if patterns.is_empty() {
        std::mem::take(&mut session.subscribed_patterns)
            .into_iter()
            .collect()
    } else {
        patterns
    };
    let mut responses = Vec::new();
    if to_process.is_empty() && session.subscribed_patterns.is_empty() {
        responses.push(RespValue::Array(vec![
            RespValue::BulkString("punsubscribe".into()),
            RespValue::Null,
            RespValue::Integer(session.subscribed_channels.len() as i64),
        ]));
    } else {
        for pattern in &to_process {
            if session.subscribed_patterns.remove(pattern) {
                let total_subs =
                    session.subscribed_channels.len() + session.subscribed_patterns.len();
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString("punsubscribe".into()),
                    RespValue::BulkString(pattern.clone()),
                    RespValue::Integer(total_subs as i64),
                ]));
            }
        }
    }
    session.pubsub_receivers.retain(|r| match r {
        SubscriptionReceiver::Pattern(p, _) => session.subscribed_patterns.contains(p),
        _ => true,
    });
    if session.subscribed_patterns.is_empty() {
        session.is_pattern_subscribed = false;
    }
    Ok(RouteResponse::Multiple(responses))
}
