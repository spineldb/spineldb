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
use tracing::debug;

/// The default capacity of the replication backlog in bytes. A larger backlog allows
/// replicas to be disconnected for longer periods before requiring a full resync.
const BACKLOG_CAPACITY: usize = 2 * 1024 * 1024; // 2MB

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
    /// Creates a new `ReplicationBacklog` and returns it along with a `watch::Receiver`.
    /// The receiver can be cloned by any task that needs to monitor changes to the
    /// primary's total replication offset.
    pub fn new() -> (Self, watch::Receiver<u64>) {
        let (tx, rx) = watch::channel(0u64);
        (
            Self {
                inner: Arc::new(Mutex::new(Inner {
                    // Pre-allocate a reasonable capacity for the VecDeque to reduce reallocations.
                    buffer: VecDeque::with_capacity(16384),
                    first_offset: 0,
                    capacity: BACKLOG_CAPACITY,
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
                let removed_len = removed_frame.encode_to_vec().unwrap_or_default().len();
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
}
