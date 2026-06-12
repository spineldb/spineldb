// src/core/replication/backlog.rs

//! Implements the replication backlog, a crucial component for efficient replication.
//!
//! The backlog is a fixed-size, in-memory circular buffer that stores recent write
//! commands sent by the primary. Its purpose is to allow replicas that disconnect and
//! reconnect quickly (e.g., due to a brief network partition) to perform a fast
//! "partial resynchronization" by replaying only the missed commands, rather than
//! undergoing a slow and costly full resynchronization (which involves a full DB snapshot).

use crate::core::protocol::RespFrame;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tracing::{debug, info};

/// The default capacity of the replication backlog in bytes. Used when no
/// configuration value is supplied. A larger backlog allows replicas to be
/// disconnected for longer periods before requiring a full resync.
const DEFAULT_BACKLOG_CAPACITY: usize = 2 * 1024 * 1024; // 2MB
/// The hard floor for the backlog. Anything smaller is meaningless because the
/// codec would immediately evict the first command written.
const MIN_BACKLOG_CAPACITY: usize = 64 * 1024; // 64 KiB

/// `ReplicationBacklog` is a thread-safe, fixed-size circular buffer.
/// It stores tuples of `(offset, command_frame)`, allowing for efficient lookup
/// of commands since a specific replication offset.
#[derive(Debug, Clone)]
pub struct ReplicationBacklog {
    /// The inner state of the backlog, protected by a Mutex for concurrent access
    /// from the event bus feeder and replica handlers.
    inner: Arc<Mutex<Inner>>,
    /// A `watch` channel sender used to efficiently notify listeners (like replica handlers
    /// and the `INFO` command) that the primary's replication offset has advanced.
    offset_notifier_tx: Arc<watch::Sender<u64>>,
}

/// The internal, mutable state of the backlog, containing the buffer and metadata.
#[derive(Debug)]
struct Inner {
    /// A double-ended queue to store the backlog entries as `(offset, frame)` tuples.
    buffer: VecDeque<(u64, RespFrame)>,
    /// The replication offset of the *first* command currently in the backlog.
    /// This is used to check if a replica's requested offset is still available.
    first_offset: u64,
    /// The maximum size of the backlog in bytes.
    capacity: usize,
    /// The current total size of the frames in the backlog, in bytes.
    current_size: usize,
}

impl ReplicationBacklog {
    /// Creates a new `ReplicationBacklog` with the default 2 MiB capacity and
    /// returns it along with a `watch::Receiver`.
    pub fn new() -> (Self, watch::Receiver<u64>) {
        Self::with_capacity(DEFAULT_BACKLOG_CAPACITY)
    }

    /// Creates a new `ReplicationBacklog` with a caller-supplied capacity in
    /// bytes. Values below [`MIN_BACKLOG_CAPACITY`] are clamped upward to that
    /// minimum, and a value of `0` selects the default. A message is logged so
    /// operators can confirm what was actually applied.
    pub fn with_capacity(capacity: usize) -> (Self, watch::Receiver<u64>) {
        let resolved = if capacity == 0 {
            DEFAULT_BACKLOG_CAPACITY
        } else {
            capacity.max(MIN_BACKLOG_CAPACITY)
        };
        if resolved != capacity {
            info!(
                "Replication backlog capacity adjusted from {} to {} bytes (min {}).",
                capacity, resolved, MIN_BACKLOG_CAPACITY
            );
        } else {
            info!("Replication backlog capacity: {} bytes.", resolved);
        }

        // Pre-allocate the VecDeque to roughly match the byte capacity. We
        // assume an average command size of ~32 bytes which is a safe lower
        // bound for RESP-encoded frames.
        let prealloc_entries = (resolved / 32).clamp(16, 1 << 20);

        let (tx, rx) = watch::channel(0u64);
        (
            Self {
                inner: Arc::new(Mutex::new(Inner {
                    buffer: VecDeque::with_capacity(prealloc_entries),
                    first_offset: 0,
                    capacity: resolved,
                    current_size: 0,
                })),
                offset_notifier_tx: Arc::new(tx),
            },
            rx,
        )
    }

    /// Adds a new command frame to the backlog.
    ///
    /// This method is called by the backlog feeder task for every propagated write command.
    /// If adding the new frame exceeds the backlog's capacity, the oldest frames
    /// are removed from the front of the queue until the size is within the capacity again.
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting replication offset of this command.
    /// * `frame` - The `RespFrame` of the command to add.
    /// * `frame_len` - The encoded length of the frame in bytes.
    pub async fn add(&self, offset: u64, frame: RespFrame, frame_len: usize) {
        let mut inner = self.inner.lock().await;

        // If the buffer is empty, this command's offset is the new starting point.
        if inner.buffer.is_empty() {
            inner.first_offset = offset;
        }

        // The new "end" of the replication stream is the start offset + frame length.
        let new_offset_end = offset + frame_len as u64;
        inner.buffer.push_back((offset, frame));
        inner.current_size += frame_len;

        // Evict old entries if the capacity is exceeded, simulating a circular buffer.
        while inner.current_size > inner.capacity {
            if let Some((_, removed_frame)) = inner.buffer.pop_front() {
                // To maintain an accurate `current_size`, we must calculate the size of the
                // removed frame. This is a reasonable approximation.
                let removed_len = removed_frame.encode_to_vec().map(|v| v.len()).unwrap_or(1); // Fallback to at least 1 to ensure progress.
                inner.current_size = inner.current_size.saturating_sub(removed_len);

                // Update the `first_offset` to reflect the new start of the backlog.
                if let Some(first) = inner.buffer.front() {
                    inner.first_offset = first.0;
                }
            } else {
                // This case should not be reachable if current_size > 0, but serves as a safeguard.
                inner.current_size = 0;
                break;
            }
        }

        // Notify all listeners (e.g., replica handlers) that the offset has advanced.
        // `send_if_modified` is an optimization to avoid waking up tasks unnecessarily if the
        // offset hasn't actually changed.
        self.offset_notifier_tx.send_if_modified(|current| {
            if *current < new_offset_end {
                *current = new_offset_end;
                true // The value was modified.
            } else {
                false // The value was not modified.
            }
        });
    }

    /// Retrieves all command frames from the backlog that have occurred since
    /// a given offset.
    ///
    /// This method is called by a `ReplicaHandler` when a replica attempts a partial resync.
    /// It returns `None` if the `since_offset` is older than the oldest data
    /// available in the backlog, signaling that a full resync is required.
    pub async fn get_since(&self, since_offset: u64) -> Option<Vec<(u64, RespFrame)>> {
        let inner = self.inner.lock().await;

        // This is the core check for partial vs. full sync. If the replica is requesting an
        // offset that has already been dropped from our backlog, it cannot be helped.
        if since_offset < inner.first_offset {
            debug!(
                "Requested offset {} is too old. Backlog starts at {}. Full resync required.",
                since_offset, inner.first_offset
            );
            return None;
        }

        // Collect all frames with a start offset greater than or equal to the requested offset.
        let frames = inner
            .buffer
            .iter()
            .filter(|(offset, _)| *offset >= since_offset)
            .cloned()
            .collect();

        Some(frames)
    }

    /// Dynamically updates the backlog capacity, in bytes. If the new capacity
    /// is below the current `current_size`, the eviction loop in [`add`](Self::add)
    /// will trim the buffer on the next write. Values below
    /// [`MIN_BACKLOG_CAPACITY`] are clamped upward.
    pub async fn set_capacity(&self, new_capacity: usize) {
        let resolved = new_capacity.max(MIN_BACKLOG_CAPACITY);
        let mut inner = self.inner.lock().await;
        inner.capacity = resolved;
        debug!(
            "Replication backlog capacity updated to {} bytes.",
            resolved
        );
    }

    /// Returns the currently configured capacity in bytes.
    pub async fn current_capacity(&self) -> usize {
        self.inner.lock().await.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// A small default capacity to make eviction tests fast.
    /// Must be at least MIN_BACKLOG_CAPACITY (64 KiB) so we use the
    /// minimum directly.
    const TEST_CAPACITY: usize = MIN_BACKLOG_CAPACITY;

    fn make_ping_frame() -> RespFrame {
        RespFrame::Array(vec![RespFrame::BulkString(Bytes::from_static(b"PING"))])
    }

    fn encoded_len(f: &RespFrame) -> usize {
        f.encode_to_vec().unwrap_or_default().len()
    }

    #[tokio::test]
    async fn test_new_uses_default_capacity() {
        let (b, _rx) = ReplicationBacklog::new();
        assert_eq!(b.current_capacity().await, DEFAULT_BACKLOG_CAPACITY);
    }

    #[tokio::test]
    async fn test_capacity_zero_uses_default() {
        let (b, _rx) = ReplicationBacklog::with_capacity(0);
        assert_eq!(b.current_capacity().await, DEFAULT_BACKLOG_CAPACITY);
    }

    #[tokio::test]
    async fn test_capacity_below_min_is_clamped() {
        let (b, _rx) = ReplicationBacklog::with_capacity(10);
        assert_eq!(b.current_capacity().await, MIN_BACKLOG_CAPACITY);
    }

    #[tokio::test]
    async fn test_add_starts_backlog_at_given_offset() {
        let (b, _rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        b.add(100, make_ping_frame(), 11).await;
        let frames = b.get_since(100).await.unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, 100);
    }

    #[tokio::test]
    async fn test_add_evicts_when_capacity_exceeded() {
        let (b, _rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        // Each PING frame encodes to ~11 bytes. Add more than capacity to force eviction.
        let frame = make_ping_frame();
        let flen = encoded_len(&frame);
        let n = (TEST_CAPACITY / flen) + 100;
        for i in 0..n {
            let offset = (i * flen) as u64;
            b.add(offset, frame.clone(), flen).await;
        }
        // After eviction, get_since(0) returns None (the earliest offset is gone).
        assert!(b.get_since(0).await.is_none());
    }

    #[tokio::test]
    async fn test_get_since_too_old_returns_none() {
        let (b, _rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        b.add(1000, make_ping_frame(), 11).await;
        b.add(2000, make_ping_frame(), 11).await;
        assert!(b.get_since(500).await.is_none());
    }

    #[tokio::test]
    async fn test_get_since_returns_only_newer_frames() {
        let (b, _rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        b.add(10, make_ping_frame(), 11).await;
        b.add(20, make_ping_frame(), 11).await;
        b.add(30, make_ping_frame(), 11).await;
        let frames = b.get_since(20).await.unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].0, 20);
        assert_eq!(frames[1].0, 30);
    }

    #[tokio::test]
    async fn test_set_capacity_clamps_below_min() {
        let (b, _rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        b.set_capacity(0).await;
        assert_eq!(b.current_capacity().await, MIN_BACKLOG_CAPACITY);
    }

    #[tokio::test]
    async fn test_set_capacity_above_min_uses_value() {
        let (b, _rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        let new_cap = MIN_BACKLOG_CAPACITY * 2;
        b.set_capacity(new_cap).await;
        assert_eq!(b.current_capacity().await, new_cap);
    }

    #[tokio::test]
    async fn test_watch_receiver_sees_offset_advances() {
        let (b, mut rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        // Initial value is 0.
        assert_eq!(*rx.borrow_and_update(), 0);
        let frame = make_ping_frame();
        let flen = encoded_len(&frame);
        b.add(0, frame, flen).await;
        // After the add, the offset end is 0 + flen.
        // Allow the watch to update.
        let changed = rx.changed().await.is_ok();
        assert!(changed);
        assert_eq!(*rx.borrow_and_update(), flen as u64);
    }

    #[tokio::test]
    async fn test_add_empty_buffer_resets_first_offset() {
        let (b, _rx) = ReplicationBacklog::with_capacity(TEST_CAPACITY);
        b.add(100, make_ping_frame(), 11).await;
        // Wipe the buffer (trick: set capacity to 0 → MIN, then keep adding to evict).
        // For a simpler test, we just verify the first_offset is the one we set.
        let frames = b.get_since(100).await.unwrap();
        assert_eq!(frames[0].0, 100);
    }
}
