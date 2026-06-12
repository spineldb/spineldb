// src/core/handler/actions/pubsub.rs

use crate::connection::{SessionState, SubscriptionReceiver};
use crate::core::database::Db;
use crate::core::handler::command_router::RouteResponse;
use crate::core::state::ServerState;
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
            let was_removed = session.subscribed_channels.remove(name);
            let total_subs = session.subscribed_channels.len() + session.subscribed_patterns.len();
            if was_removed {
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString("unsubscribe".into()),
                    RespValue::BulkString(name.clone()),
                    RespValue::Integer(total_subs as i64),
                ]));
            } else {
                // Per Redis protocol, unsubscribing from a non-existent channel still returns a response.
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
            let was_removed = session.subscribed_patterns.remove(pattern);
            let total_subs = session.subscribed_channels.len() + session.subscribed_patterns.len();
            if was_removed {
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString("punsubscribe".into()),
                    RespValue::BulkString(pattern.clone()),
                    RespValue::Integer(total_subs as i64),
                ]));
            } else {
                // Per Redis protocol, punsubscribing from a non-existent pattern still returns a response.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::SessionState;
    use std::collections::HashSet;

    fn make_session() -> SessionState {
        SessionState {
            is_authenticated: true,
            is_in_transaction: false,
            is_asking: false,
            is_subscribed: false,
            is_pattern_subscribed: false,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
            pubsub_receivers: Vec::new(),
            current_db_index: 0,
            authenticated_user: None,
        }
    }

    #[test]
    fn test_unsubscribe_empty_no_subs() {
        let mut session = make_session();
        let resp = handle_unsubscribe(vec![], &mut session).unwrap();
        match resp {
            RouteResponse::Multiple(arr) => {
                assert_eq!(arr.len(), 1);
                if let RespValue::Array(inner) = &arr[0] {
                    assert_eq!(inner.len(), 3);
                    assert_eq!(inner[1], RespValue::Null);
                } else {
                    panic!("expected array");
                }
            }
            _ => panic!("expected Multiple"),
        }
    }

    #[test]
    fn test_unsubscribe_specific_channel() {
        let mut session = make_session();
        session.is_subscribed = true;
        session
            .subscribed_channels
            .insert(Bytes::from_static(b"news"));
        session
            .subscribed_channels
            .insert(Bytes::from_static(b"sports"));

        let resp = handle_unsubscribe(vec![Bytes::from_static(b"news")], &mut session).unwrap();
        match resp {
            RouteResponse::Multiple(arr) => {
                assert_eq!(arr.len(), 1);
                assert!(
                    !session
                        .subscribed_channels
                        .contains(&Bytes::from_static(b"news"))
                );
                assert!(
                    session
                        .subscribed_channels
                        .contains(&Bytes::from_static(b"sports"))
                );
            }
            _ => panic!("expected Multiple"),
        }
    }

    #[test]
    fn test_unsubscribe_nonexistent_channel() {
        let mut session = make_session();
        let resp = handle_unsubscribe(vec![Bytes::from_static(b"nope")], &mut session).unwrap();
        match resp {
            RouteResponse::Multiple(arr) => {
                // Per Redis protocol, unsubscribing from a non-existent channel still returns a response.
                assert_eq!(arr.len(), 1);
                if let RespValue::Array(inner) = &arr[0] {
                    assert_eq!(inner.len(), 3);
                    assert_eq!(inner[0], RespValue::BulkString("unsubscribe".into()));
                    assert_eq!(inner[1], RespValue::BulkString(Bytes::from_static(b"nope")));
                    assert_eq!(inner[2], RespValue::Integer(0));
                } else {
                    panic!("expected array");
                }
            }
            _ => panic!("expected Multiple"),
        }
    }

    #[test]
    fn test_unsubscribe_all_clears_subscription_flag() {
        let mut session = make_session();
        session.is_subscribed = true;
        session.subscribed_channels.insert(Bytes::from_static(b"a"));

        let _ = handle_unsubscribe(vec![], &mut session).unwrap();
        assert!(session.subscribed_channels.is_empty());
        assert!(!session.is_subscribed);
    }

    #[test]
    fn test_punsubscribe_empty_no_subs() {
        let mut session = make_session();
        let resp = handle_punsubscribe(vec![], &mut session).unwrap();
        match resp {
            RouteResponse::Multiple(arr) => {
                assert_eq!(arr.len(), 1);
                if let RespValue::Array(inner) = &arr[0] {
                    assert_eq!(inner[1], RespValue::Null);
                } else {
                    panic!("expected array");
                }
            }
            _ => panic!("expected Multiple"),
        }
    }

    #[test]
    fn test_punsubscribe_specific_pattern() {
        let mut session = make_session();
        session.is_pattern_subscribed = true;
        session
            .subscribed_patterns
            .insert(Bytes::from_static(b"news.*"));
        session
            .subscribed_patterns
            .insert(Bytes::from_static(b"sports.*"));

        let resp = handle_punsubscribe(vec![Bytes::from_static(b"news.*")], &mut session).unwrap();
        match resp {
            RouteResponse::Multiple(arr) => {
                assert_eq!(arr.len(), 1);
                assert!(
                    !session
                        .subscribed_patterns
                        .contains(&Bytes::from_static(b"news.*"))
                );
                assert!(
                    session
                        .subscribed_patterns
                        .contains(&Bytes::from_static(b"sports.*"))
                );
            }
            _ => panic!("expected Multiple"),
        }
    }

    #[test]
    fn test_punsubscribe_all_clears_flag() {
        let mut session = make_session();
        session.is_pattern_subscribed = true;
        session
            .subscribed_patterns
            .insert(Bytes::from_static(b"a.*"));

        let _ = handle_punsubscribe(vec![], &mut session).unwrap();
        assert!(session.subscribed_patterns.is_empty());
        assert!(!session.is_pattern_subscribed);
    }

    #[test]
    fn test_unsubscribe_response_count_decrements() {
        let mut session = make_session();
        session.is_subscribed = true;
        session.subscribed_channels.insert(Bytes::from_static(b"a"));
        session.subscribed_channels.insert(Bytes::from_static(b"b"));

        let resp = handle_unsubscribe(vec![Bytes::from_static(b"a")], &mut session).unwrap();
        if let RouteResponse::Multiple(arr) = resp
            && let RespValue::Array(inner) = &arr[0]
            && let RespValue::Integer(count) = inner[2]
        {
            assert_eq!(count, 1); // 1 remaining sub
        }
    }
}
