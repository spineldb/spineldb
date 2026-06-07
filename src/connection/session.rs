// src/connection/session.rs

//! Defines the state associated with a single client session.

use crate::core::acl::user::AclUser;
use crate::core::pubsub::PMessage;
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Holds the state specific to a single client session.
#[derive(Debug)]
pub struct SessionState {
    /// True if the client has successfully authenticated.
    pub is_authenticated: bool,
    /// True if the client is within a `MULTI`/`EXEC` block.
    pub is_in_transaction: bool,
    /// True for the one command immediately following an `ASKING` command.
    pub is_asking: bool,
    /// True if the client is subscribed to one or more channels.
    pub is_subscribed: bool,
    /// True if the client is subscribed to one or more patterns.
    pub is_pattern_subscribed: bool,
    /// The set of channels the client is directly subscribed to.
    pub subscribed_channels: HashSet<Bytes>,
    /// The set of patterns the client is subscribed to.
    pub subscribed_patterns: HashSet<Bytes>,
    /// A collection of `broadcast::Receiver`s for active subscriptions.
    pub pubsub_receivers: Vec<SubscriptionReceiver>,
    /// The index of the database the client is currently using.
    pub current_db_index: usize,
    /// The `AclUser` associated with the authenticated session, if any.
    pub authenticated_user: Option<Arc<AclUser>>,
}

/// An enum holding a receiver for either a channel or pattern subscription.
#[derive(Debug)]
pub enum SubscriptionReceiver {
    /// A receiver for a specific channel.
    Channel(Bytes, broadcast::Receiver<Bytes>),
    /// A receiver for a glob-style pattern.
    Pattern(Bytes, broadcast::Receiver<PMessage>),
}

impl SessionState {
    /// Creates a new `SessionState` with default values.
    pub(crate) fn new(is_auth_required: bool, acl_enabled: bool) -> Self {
        Self {
            is_authenticated: !is_auth_required && !acl_enabled,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_session_no_auth_no_acl_is_authenticated() {
        let s = SessionState::new(false, false);
        assert!(s.is_authenticated);
        assert!(!s.is_in_transaction);
        assert!(!s.is_asking);
        assert!(!s.is_subscribed);
        assert!(!s.is_pattern_subscribed);
        assert!(s.subscribed_channels.is_empty());
        assert!(s.subscribed_patterns.is_empty());
        assert!(s.pubsub_receivers.is_empty());
        assert_eq!(s.current_db_index, 0);
        assert!(s.authenticated_user.is_none());
    }

    #[test]
    fn test_new_session_with_auth_required_is_not_authenticated() {
        let s = SessionState::new(true, false);
        assert!(!s.is_authenticated);
    }

    #[test]
    fn test_new_session_with_acl_enabled_is_not_authenticated() {
        let s = SessionState::new(false, true);
        assert!(!s.is_authenticated);
    }

    #[test]
    fn test_new_session_with_both_auth_and_acl_is_not_authenticated() {
        let s = SessionState::new(true, true);
        assert!(!s.is_authenticated);
    }

    #[test]
    fn test_session_state_db_index_mutable() {
        let mut s = SessionState::new(false, false);
        s.current_db_index = 5;
        assert_eq!(s.current_db_index, 5);
    }

    #[test]
    fn test_session_state_channels_mutable() {
        let mut s = SessionState::new(false, false);
        s.subscribed_channels.insert(Bytes::from_static(b"ch1"));
        s.subscribed_channels.insert(Bytes::from_static(b"ch2"));
        s.is_subscribed = true;
        assert_eq!(s.subscribed_channels.len(), 2);
        assert!(s.is_subscribed);
    }
}
