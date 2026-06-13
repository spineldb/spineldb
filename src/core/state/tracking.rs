// src/core/state/tracking.rs

//! State for CLIENT TRACKING (client-side caching invalidation).

use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc;

/// A message sent to a tracking client when a tracked key is invalidated.
#[derive(Debug, Clone)]
pub struct InvalidationMessage {
    /// The database index where the key was modified.
    pub db_index: usize,
    /// The keys that were invalidated.
    pub keys: Vec<Bytes>,
}

/// Holds all state related to CLIENT TRACKING for client-side caching.
#[derive(Debug)]
pub struct TrackingState {
    /// Maps session_id -> set of tracked key names (for standard tracking).
    /// Keys tracked by each client session.
    pub tracked_keys: Arc<DashMap<u64, HashSet<Bytes>>>,

    /// Maps key_name -> set of session_ids (for BCAST tracking).
    /// Which clients are tracking each key in broadcast mode.
    pub bcast_keys: Arc<DashMap<Bytes, HashSet<u64>>>,

    /// Maps session_id -> channel sender for sending invalidation Push messages.
    /// Each tracked client gets a channel to receive invalidation notifications.
    pub invalidation_channels: Arc<DashMap<u64, mpsc::Sender<InvalidationMessage>>>,

    /// Set of session_ids that have tracking enabled (any mode).
    pub tracking_enabled: Arc<DashMap<u64, bool>>,
}

impl Default for TrackingState {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackingState {
    pub fn new() -> Self {
        Self {
            tracked_keys: Arc::new(DashMap::new()),
            bcast_keys: Arc::new(DashMap::new()),
            invalidation_channels: Arc::new(DashMap::new()),
            tracking_enabled: Arc::new(DashMap::new()),
        }
    }

    /// Enables tracking for a client session.
    pub fn enable_tracking(&self, session_id: u64) {
        self.tracking_enabled.insert(session_id, true);
    }

    /// Disables tracking for a client session and cleans up all its state.
    pub fn disable_tracking(&self, session_id: u64) {
        self.tracking_enabled.remove(&session_id);
        self.tracked_keys.remove(&session_id);
        self.invalidation_channels.remove(&session_id);

        // Remove this session from all bcast_keys entries
        for mut entry in self.bcast_keys.iter_mut() {
            entry.value_mut().remove(&session_id);
        }
        // Clean up empty entries
        self.bcast_keys.retain(|_, sessions| !sessions.is_empty());
    }

    /// Tracks a specific key for a client session (standard mode).
    pub fn track_key(&self, session_id: u64, key: Bytes) {
        if let Some(mut entry) = self.tracked_keys.get_mut(&session_id) {
            entry.insert(key);
        } else {
            let mut set = HashSet::new();
            set.insert(key);
            self.tracked_keys.insert(session_id, set);
        }
    }

    /// Registers a key in broadcast mode tracking.
    pub fn bcast_track_key(&self, session_id: u64, key: Bytes) {
        if let Some(mut entry) = self.bcast_keys.get_mut(&key) {
            entry.insert(session_id);
        } else {
            let mut set = HashSet::new();
            set.insert(session_id);
            self.bcast_keys.insert(key, set);
        }
    }

    /// Removes a specific key from a client's tracked keys (standard mode).
    pub fn untrack_key(&self, session_id: u64, key: &Bytes) {
        if let Some(mut entry) = self.tracked_keys.get_mut(&session_id) {
            entry.remove(key);
        }
    }

    /// Sets the invalidation channel for a client.
    pub fn set_invalidation_channel(
        &self,
        session_id: u64,
        sender: mpsc::Sender<InvalidationMessage>,
    ) {
        self.invalidation_channels.insert(session_id, sender);
    }

    /// Returns all session_ids that are tracking the given key.
    pub fn get_tracking_sessions_for_key(&self, key: &Bytes) -> Vec<u64> {
        let mut sessions = Vec::new();

        // Check standard tracking: find sessions that track this specific key
        for entry in self.tracked_keys.iter() {
            if entry.value().contains(key) {
                sessions.push(*entry.key());
            }
        }

        // Check broadcast mode: find sessions registered for this key
        if let Some(entry) = self.bcast_keys.get(key) {
            for session_id in entry.value() {
                if !sessions.contains(session_id) {
                    sessions.push(*session_id);
                }
            }
        }

        sessions
    }

    /// Sends an invalidation message to all sessions tracking the given keys.
    pub async fn invalidate_keys(&self, db_index: usize, keys: &[Bytes]) {
        if keys.is_empty() {
            return;
        }

        let mut all_sessions: Vec<u64> = Vec::new();
        for key in keys {
            let sessions = self.get_tracking_sessions_for_key(key);
            for s in sessions {
                if !all_sessions.contains(&s) {
                    all_sessions.push(s);
                }
            }
        }

        let msg = InvalidationMessage {
            db_index,
            keys: keys.to_vec(),
        };

        for session_id in all_sessions {
            if let Some(entry) = self.invalidation_channels.get(&session_id) {
                // Best-effort send; if channel is full or dropped, just ignore
                let _ = entry.value().try_send(msg.clone());
            }
        }
    }

    /// Cleans up tracking state for a disconnected client.
    pub fn cleanup_session(&self, session_id: u64) {
        self.disable_tracking(session_id);
    }
}
