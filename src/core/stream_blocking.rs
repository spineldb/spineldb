// src/core/stream_blocking.rs

//! Manages clients that are blocked waiting for data on one or more streams,
//! primarily for the `XREAD[GROUP]` command with the `BLOCK` option. This
//! implementation is cluster-aware, ensuring blocked clients are correctly
//! handled during slot migrations.

use crate::core::cluster::slot::get_slot;
use crate::core::database::ExecutionContext;
use crate::core::state::ServerState;
use crate::core::storage::data_types::DataValue;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::debug;

/// The result of a stream blocking operation, indicating whether the client was
/// woken up by new data, timed out, or needs to be moved to another node.
#[derive(Debug)]
pub enum StreamBlockerResult {
    /// The client was woken up because new data arrived on a stream.
    Woken,
    /// The blocking operation timed out.
    TimedOut,
    /// The slot for a watched stream key was migrated to another cluster node.
    Moved(u16),
}

/// The waker for stream blocking only needs to signal that data has arrived.
type Waker = oneshot::Sender<()>;

/// A shareable, thread-safe waker. The `Option` allows it to be `take()`-n,
/// ensuring it is only used once.
type SharedWaker = Arc<Mutex<Option<Waker>>>;

/// Holds information about a waiting client, including its session ID for cleanup.
#[derive(Clone, Debug)]
struct WaiterInfo {
    session_id: u64,
    waker: SharedWaker,
}

/// Manages all clients currently blocked on stream commands.
#[derive(Debug, Default)]
pub struct StreamBlockerManager {
    // Key: The name of the stream being watched.
    // Value: A queue of waiters for clients waiting on this stream.
    waiters: DashMap<Bytes, VecDeque<WaiterInfo>>,
}

impl StreamBlockerManager {
    pub fn new() -> Self {
        Default::default()
    }

    /// The main orchestrator for blocking on stream keys.
    ///
    /// This function implements a race-condition-free blocking pattern:
    /// 1. Records the current state of the streams (sequence numbers).
    /// 2. Registers a waker *before* releasing the database locks.
    /// 3. Releases locks and waits for a notification or timeout.
    /// 4. After waking, re-acquires locks and verifies that the stream state has
    ///    actually changed to prevent spurious wakeups.
    /// 5. Cleans up the waker upon completion.
    pub async fn block_on(
        self: &Arc<Self>,
        ctx: &mut ExecutionContext<'_>,
        keys: &[Bytes],
        wait_timeout: Duration,
    ) -> StreamBlockerResult {
        // --- Phase 1: Pre-block registration ---
        let (tx, mut rx) = oneshot::channel();
        let shared_waker = Arc::new(Mutex::new(Some(tx)));
        let waiter_info = WaiterInfo {
            session_id: ctx.session_id,
            waker: shared_waker.clone(),
        };

        // Record the sequence number of each stream *before* releasing the lock.
        // This is crucial for verifying a real change after waking up.
        let mut initial_sequences = HashMap::new();
        if let crate::core::database::ExecutionLocks::Multi { guards } = &mut ctx.locks {
            for key in keys {
                let shard_index = ctx.db.get_shard_index(key);
                let sequence = guards
                    .get(&shard_index)
                    .and_then(|guard| guard.peek(key))
                    .and_then(|entry| match &entry.data {
                        DataValue::Stream(s) => Some(s.sequence_number.load(Ordering::Relaxed)),
                        _ => None,
                    })
                    .unwrap_or(0);
                initial_sequences.insert(key.clone(), sequence);
            }
        }

        // CRITICAL: Register waker BEFORE releasing locks to prevent a race condition.
        for key in keys {
            self.waiters
                .entry(key.clone())
                .or_default()
                .push_back(waiter_info.clone());
        }
        debug!(
            "Session {}: Registered to block on streams: {:?}",
            ctx.session_id, keys
        );

        // --- Phase 2: Release locks and enter blocking wait ---
        ctx.release_locks();
        debug!(
            "Session {}: Locks released. Awaiting notification.",
            ctx.session_id
        );

        let block_result = self
            .wait_with_polling(keys, &mut rx, wait_timeout, &ctx.state)
            .await;

        // --- Phase 3: Cleanup and post-wakeup verification ---
        self.remove_waiter(keys, &shared_waker);
        debug!("Session {}: Stream waiter cleaned up.", ctx.session_id);

        // If woken up, re-acquire locks and verify that the stream has actually changed.
        // This prevents spurious wakeups from causing a client to re-read old data.
        if matches!(block_result, StreamBlockerResult::Woken) {
            if ctx.reacquire_locks_for_command().await.is_err() {
                return StreamBlockerResult::TimedOut; // Assume failure if locks can't be reacquired
            }
            let mut state_changed = false;
            if let crate::core::database::ExecutionLocks::Multi { guards } = &mut ctx.locks {
                for key in keys {
                    let initial_seq = initial_sequences.get(key).unwrap_or(&0);
                    let shard_index = ctx.db.get_shard_index(key);
                    let current_seq = guards
                        .get(&shard_index)
                        .and_then(|guard| guard.peek(key))
                        .and_then(|entry| match &entry.data {
                            DataValue::Stream(s) => Some(s.sequence_number.load(Ordering::Relaxed)),
                            _ => None,
                        })
                        .unwrap_or(0);

                    if current_seq > *initial_seq {
                        state_changed = true;
                        break;
                    }
                }
            }
            if !state_changed {
                return StreamBlockerResult::TimedOut; // Spurious wakeup, treat as timeout.
            }
        }

        block_result
    }

    /// The actual waiting logic, supporting both cluster and standalone modes.
    async fn wait_with_polling(
        &self,
        keys: &[Bytes],
        rx: &mut oneshot::Receiver<()>,
        wait_timeout: Duration,
        state: &Arc<ServerState>,
    ) -> StreamBlockerResult {
        // In standalone mode, use a simple and efficient timeout.
        let Some(cluster_state) = &state.cluster else {
            return match timeout(wait_timeout, rx).await {
                Ok(_) => StreamBlockerResult::Woken,
                _ => StreamBlockerResult::TimedOut,
            };
        };

        // In cluster mode, use a "lazy polling" loop to handle slot migrations.
        const POLLING_TIMEOUT: Duration = Duration::from_millis(500);
        let deadline = Instant::now() + wait_timeout;
        let my_slot = get_slot(&keys[0]); // All keys must be in the same slot.

        loop {
            let now = Instant::now();
            if now >= deadline {
                return StreamBlockerResult::TimedOut;
            }
            let time_left = deadline - now;
            let current_timeout = POLLING_TIMEOUT.min(time_left);

            match timeout(current_timeout, &mut *rx).await {
                Ok(_) => return StreamBlockerResult::Woken,
                Err(_) => {
                    // Polling timeout reached. Check cluster state.
                    if !cluster_state.i_own_slot(my_slot) {
                        debug!(
                            "Slot {} for stream moved while client was blocked. Aborting wait.",
                            my_slot
                        );
                        return StreamBlockerResult::Moved(my_slot);
                    }
                }
            }
        }
    }

    /// Wakes up all clients waiting on a specific stream. Called by `XADD`.
    pub fn notify(&self, key: &Bytes) {
        if let Some(mut queue) = self.waiters.get_mut(key) {
            if queue.is_empty() {
                return;
            }
            debug!(
                "Notifying {} waiters for stream '{}'",
                queue.len(),
                String::from_utf8_lossy(key)
            );

            // Wake up all waiters. `take()` ensures each is only used once.
            while let Some(info) = queue.pop_front() {
                if let Ok(mut guard) = info.waker.lock()
                    && let Some(waker) = guard.take()
                {
                    let _ = waker.send(());
                }
            }
        }
    }

    /// Notifies and removes all waiters for a stream that is being deleted.
    /// Called by `DEL`/`UNLINK`.
    pub fn notify_and_remove_all(&self, key: &Bytes) {
        if let Some((_, mut queue)) = self.waiters.remove(key) {
            while let Some(info) = queue.pop_front() {
                if let Ok(mut guard) = info.waker.lock()
                    && let Some(waker) = guard.take()
                {
                    let _ = waker.send(());
                }
            }
        }
    }

    /// Cleans up a specific waker from all associated key queues after it's been
    /// used or has timed out.
    fn remove_waiter(&self, keys: &[Bytes], waker_to_remove: &SharedWaker) {
        for key in keys {
            if let Some(mut queue) = self.waiters.get_mut(key) {
                queue.retain(|info| !Arc::ptr_eq(&info.waker, waker_to_remove));
                if queue.is_empty() {
                    drop(queue);
                    self.waiters.remove(key);
                }
            }
        }
    }

    /// Removes all wakers for a given session_id.
    /// Called when a client connection is closed to prevent dangling wakers.
    pub fn remove_waiters_for_session(&self, session_id: u64) {
        self.waiters.iter_mut().for_each(|mut queue| {
            queue.retain(|info| info.session_id != session_id);
        });
        self.waiters.retain(|_, queue| !queue.is_empty());
        debug!(
            "Removed any pending stream blockers for session_id {}.",
            session_id
        );
    }
}
