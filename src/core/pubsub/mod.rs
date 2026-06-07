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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::TryRecvError;

    #[test]
    fn test_new_manager_is_empty() {
        let mgr = PubSubManager::new();
        assert!(mgr.get_all_channels().is_empty());
        assert_eq!(mgr.get_pattern_subscriber_count(), 0);
    }

    #[test]
    fn test_subscribe_creates_channel() {
        let mgr = PubSubManager::new();
        let channel = Bytes::from_static(b"news");
        let _rx = mgr.subscribe(&channel);
        assert_eq!(mgr.get_subscriber_count(&channel), 1);
        assert_eq!(mgr.get_all_channels().len(), 1);
    }

    #[test]
    fn test_subscribe_multiple_to_same_channel() {
        let mgr = PubSubManager::new();
        let channel = Bytes::from_static(b"news");
        let _r1 = mgr.subscribe(&channel);
        let _r2 = mgr.subscribe(&channel);
        assert_eq!(mgr.get_subscriber_count(&channel), 2);
    }

    #[test]
    fn test_subscribe_different_channels() {
        let mgr = PubSubManager::new();
        let _r1 = mgr.subscribe(&Bytes::from_static(b"a"));
        let _r2 = mgr.subscribe(&Bytes::from_static(b"b"));
        assert_eq!(mgr.get_all_channels().len(), 2);
    }

    #[test]
    fn test_unsubscribe_does_not_remove_channel() {
        let mgr = PubSubManager::new();
        let channel = Bytes::from_static(b"news");
        let _rx = mgr.subscribe(&channel);
        mgr.unsubscribe(&channel);
        // The unsubscribe method is currently a no-op for removal;
        // channel exists in map but receiver count goes to 0 when `_rx` is dropped.
        assert_eq!(mgr.get_all_channels().len(), 1);
    }

    #[test]
    fn test_subscribe_pattern_creates_pattern() {
        let mgr = PubSubManager::new();
        let pat = Bytes::from_static(b"news.*");
        let _rx = mgr.subscribe_pattern(&pat);
        assert_eq!(mgr.get_pattern_subscriber_count(), 1);
    }

    #[test]
    fn test_unsubscribe_pattern_is_noop() {
        let mgr = PubSubManager::new();
        let pat = Bytes::from_static(b"news.*");
        let _rx = mgr.subscribe_pattern(&pat);
        mgr.unsubscribe_pattern(&pat);
        assert_eq!(mgr.get_pattern_subscriber_count(), 1);
    }

    #[test]
    fn test_get_subscriber_count_for_unknown_channel_is_zero() {
        let mgr = PubSubManager::new();
        assert_eq!(mgr.get_subscriber_count(&Bytes::from_static(b"nope")), 0);
    }

    #[test]
    fn test_get_pattern_subscriber_count_default_zero() {
        let mgr = PubSubManager::new();
        assert_eq!(mgr.get_pattern_subscriber_count(), 0);
    }

    #[test]
    fn test_purge_empty_channels_no_op() {
        let mgr = PubSubManager::new();
        assert_eq!(mgr.purge_empty_channels(), 0);
    }

    #[test]
    fn test_purge_empty_channels_removes_empty_entries() {
        let mgr = PubSubManager::new();
        // Subscribe and immediately drop the receiver so the channel becomes empty.
        {
            let _rx = mgr.subscribe(&Bytes::from_static(b"a"));
            let _rx2 = mgr.subscribe_pattern(&Bytes::from_static(b"a.*"));
        }
        // Now both channels should have 0 receivers.
        let purged = mgr.purge_empty_channels();
        assert_eq!(purged, 2);
        assert!(mgr.get_all_channels().is_empty());
        assert_eq!(mgr.get_pattern_subscriber_count(), 0);
    }

    #[test]
    fn test_purge_keeps_active_channels() {
        let mgr = PubSubManager::new();
        let _rx = mgr.subscribe(&Bytes::from_static(b"active"));
        // drop nothing; the receiver is still alive in the test
        let purged = mgr.purge_empty_channels();
        assert_eq!(purged, 0);
        assert_eq!(mgr.get_all_channels().len(), 1);
    }

    #[test]
    fn test_publish_to_empty_channel_returns_zero() {
        let mgr = PubSubManager::new();
        let count = mgr.publish(&Bytes::from_static(b"nobody"), Bytes::from_static(b"hi"));
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_publish_to_subscriber_delivers_message() {
        let mgr = PubSubManager::new();
        let channel = Bytes::from_static(b"c");
        let mut rx = mgr.subscribe(&channel);
        let count = mgr.publish(&channel, Bytes::from_static(b"hello"));
        assert_eq!(count, 1);
        let received = rx.recv().await.unwrap();
        assert_eq!(received, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn test_publish_to_pattern_subscriber_delivers_pmessage() {
        let mgr = PubSubManager::new();
        let pat = Bytes::from_static(b"news.*");
        let mut rx = mgr.subscribe_pattern(&pat);
        let channel = Bytes::from_static(b"news.weather");
        let count = mgr.publish(&channel, Bytes::from_static(b"sunny"));
        assert_eq!(count, 1);
        let (matched_pat, orig_channel, msg) = rx.recv().await.unwrap();
        assert_eq!(matched_pat, Bytes::from_static(b"news.*"));
        assert_eq!(orig_channel, Bytes::from_static(b"news.weather"));
        assert_eq!(msg, Bytes::from_static(b"sunny"));
    }

    #[tokio::test]
    async fn test_publish_to_both_direct_and_pattern() {
        let mgr = PubSubManager::new();
        let channel = Bytes::from_static(b"news.weather");
        let pat = Bytes::from_static(b"news.*");

        let _direct_rx = mgr.subscribe(&channel);
        let mut pattern_rx = mgr.subscribe_pattern(&pat);

        let count = mgr.publish(&channel, Bytes::from_static(b"rain"));
        assert_eq!(count, 2);
        // Drain one message; the other is still in the pattern_rx queue.
        let received = pattern_rx.recv().await.unwrap();
        assert_eq!(received.2, Bytes::from_static(b"rain"));
    }

    #[test]
    fn test_try_recv_on_empty_channel_returns_error() {
        let mgr = PubSubManager::new();
        let channel = Bytes::from_static(b"c");
        let mut rx = mgr.subscribe(&channel);
        // No message published yet.
        match rx.try_recv() {
            Err(TryRecvError::Empty) => {}
            other => panic!("expected Empty, got {other:?}"),
        }
    }
}
