// src/core/pubsub/mod.rs

//! The core publish-subscribe (Pub/Sub) system.
//! It manages channel and pattern subscriptions and message broadcasting.

use crate::core::commands::scan::glob_match;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tracing::debug;

// Export sub-modules.
pub mod channel_purger;
pub mod handler;

/// The capacity of each individual broadcast channel.
const CHANNEL_CAPACITY: usize = 128;

/// A type alias for messages sent to pattern subscribers.
/// The tuple contains: (matching_pattern, original_channel, message).
pub type PMessage = (Bytes, Bytes, Bytes);

/// `PubSubManager` is the central hub for all Pub/Sub functionality.
/// It uses `DashMap` for thread-safe management of channel and pattern subscriptions.
#[derive(Debug, Default)]
pub struct PubSubManager {
    /// A map from a channel name to its broadcast sender.
    channels: DashMap<Bytes, Arc<Sender<Bytes>>>,
    /// A map from a pattern to its broadcast sender for pattern-based subscriptions.
    pattern_channels: DashMap<Bytes, Arc<Sender<PMessage>>>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Default::default()
    }

    /// Subscribes a client to a specific channel.
    ///
    /// If the channel does not exist, it is created. It returns a `Receiver`
    /// that the client's connection handler will listen on.
    pub fn subscribe(&self, channel_name: &Bytes) -> Receiver<Bytes> {
        self.channels
            .entry(channel_name.clone())
            .or_insert_with(|| Arc::new(broadcast::channel(CHANNEL_CAPACITY).0))
            .value()
            .subscribe()
    }

    /// Subscribes a client to a glob-style pattern.
    ///
    /// If the pattern subscription does not exist, it is created. It returns a `Receiver`
    /// for `PMessage` tuples.
    pub fn subscribe_pattern(&self, pattern: &Bytes) -> Receiver<PMessage> {
        self.pattern_channels
            .entry(pattern.clone())
            .or_insert_with(|| Arc::new(broadcast::channel(CHANNEL_CAPACITY).0))
            .value()
            .subscribe()
    }

    /// Unsubscribes a client from a channel.
    /// The actual removal of the broadcast sender (if it becomes empty) is handled
    /// by the `purge_empty_channels` background task.
    pub fn unsubscribe(&self, _channel_name: &Bytes) {}

    /// Unsubscribes a client from a pattern.
    pub fn unsubscribe_pattern(&self, _pattern: &Bytes) {}

    /// Publishes a message to a channel.
    ///
    /// This method broadcasts the message to two groups:
    /// 1. Direct subscribers of the `channel_name`.
    /// 2. Subscribers of any pattern that matches the `channel_name`.
    ///
    /// Returns the total number of clients that received the message.
    pub fn publish(&self, channel_name: &Bytes, message: Bytes) -> usize {
        let mut receivers = 0;

        // Send to direct channel subscribers.
        if let Some(channel) = self.channels.get(channel_name) {
            // `send` returns the number of receivers the message was sent to.
            receivers += channel.send(message.clone()).unwrap_or(0);
        }

        // Send to pattern subscribers.
        for entry in self.pattern_channels.iter() {
            let pattern = entry.key();
            let sender = entry.value();
            if glob_match(pattern, channel_name) {
                // The message for pattern subscribers includes the pattern and original channel.
                let pmessage: PMessage = (pattern.clone(), channel_name.clone(), message.clone());
                receivers += sender.send(pmessage).unwrap_or(0);
            }
        }

        receivers
    }

    /// A maintenance task that removes channels and patterns that no longer have any subscribers.
    /// This prevents memory leaks from empty, unused channels.
    pub fn purge_empty_channels(&self) -> usize {
        let mut purged_count = 0;
        self.channels.retain(|_channel_name, sender| {
            if sender.receiver_count() == 0 {
                purged_count += 1;
                false // Remove the entry.
            } else {
                true // Keep the entry.
            }
        });

        self.pattern_channels.retain(|_pattern, sender| {
            if sender.receiver_count() == 0 {
                purged_count += 1;
                false
            } else {
                true
            }
        });

        if purged_count > 0 {
            debug!(
                "Purged {} empty Pub/Sub channels and patterns.",
                purged_count
            );
        }
        purged_count
    }

    /// Returns a list of all active channels.
    pub fn get_all_channels(&self) -> Vec<Bytes> {
        self.channels.iter().map(|e| e.key().clone()).collect()
    }

    /// Returns the number of subscribers for a specific channel.
    pub fn get_subscriber_count(&self, channel_name: &Bytes) -> usize {
        self.channels
            .get(channel_name)
            .map_or(0, |s| s.receiver_count())
    }

    /// Returns the total number of active pattern subscriptions.
    pub fn get_pattern_subscriber_count(&self) -> usize {
        self.pattern_channels.len()
    }
}
