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
