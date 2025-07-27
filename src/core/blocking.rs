// src/core/blocking.rs

//! Manages clients that are blocked waiting for data on list/zset keys.
//! This implementation is cluster-aware and handles slot migrations safely.
//!
//! # Core Design: Preventing Race Conditions
//!
//! The primary challenge in implementing blocking commands like `BLPOP` or `BZPOPMIN` is
//! avoiding a race condition where a client decides to block just as another client
//! pushes data, causing the first client to miss the notification.
//!
//! This module solves this by ensuring that the client performing the write
//! operation (e.g., `LPUSH`, `ZADD`), while holding the necessary data lock, is
//! responsible for atomically performing the corresponding pop and sending the
//! popped value directly to the waiting client through a `oneshot` channel (the waker).
//! This guarantees that the data is transferred from one task to another without
//! requiring the awakened client to re-acquire locks and race for the data.

use crate::core::cluster::slot::get_slot;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::commands::list::RPush;
use crate::core::commands::list::lmove::{Side, lmove_logic};
use crate::core::commands::list::logic::list_pop_logic;
use crate::core::commands::zset::zpop_logic::{PopSide, zpop_logic};
use crate::core::state::ServerState;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::db::PopDirection;
use crate::core::storage::db::zset::SortedSet;
use crate::core::{Command, RespValue, SpinelDBError};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::debug;

/// The value returned when a blocking list pop operation is successful.
#[derive(Debug, Clone)]
pub struct PoppedValue {
    pub key: Bytes,
    pub value: Bytes,
}

/// The value returned when a blocking zset pop operation is successful.
#[derive(Debug, Clone)]
pub struct ZSetPoppedValue {
    pub key: Bytes,
    pub member: Bytes,
    pub score: f64,
}

/// A generic enum to hold the woken value from either a list or zset.
#[derive(Debug, Clone)]
pub enum WokenValue {
    List(PoppedValue),
    ZSet(ZSetPoppedValue),
}

/// The result of a blocking operation, indicating the outcome.
#[derive(Debug)]
enum BlockerOutcome {
    /// The client was woken up with data.
    Woken(WokenValue),
    /// The operation timed out before any data arrived.
    TimedOut,
    /// The slot for the key moved to another node while the client was blocked.
    Moved(u16),
}

/// The waker sends the woken value, eliminating the need for a re-read.
type Waker = oneshot::Sender<WokenValue>;

/// A shareable waker struct. The `Option` allows it to be `take()`-n to prevent multiple sends.
type SharedWaker = Arc<Mutex<Option<Waker>>>;

/// Holds information about a waiting client, including its session ID for cleanup.
#[derive(Clone, Debug)]
struct WaiterInfo {
    session_id: u64,
    waker: SharedWaker,
}

/// Manages all clients currently blocked and waiting for data on list/zset keys.
#[derive(Debug, Default)]
pub struct BlockerManager {
    // Key: The name of the key being watched.
    // Value: A queue of waiters for clients waiting on this key.
    waiters: DashMap<Bytes, VecDeque<WaiterInfo>>,
}

impl BlockerManager {
    /// Creates a new, empty `BlockerManager`.
    pub fn new() -> Self {
        Default::default()
    }

    /// The main orchestrator for blocking list pop operations (`BLPOP`, `BRPOP`).
    pub async fn orchestrate_blocking_pop(
        self: &Arc<Self>,
        ctx: &mut ExecutionContext<'_>,
        keys: &[Bytes],
        direction: PopDirection,
        wait_timeout: Duration,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // --- Phase 1: Attempt a non-blocking pop across all keys ---
        for key in keys {
            let (resp, outcome) = list_pop_logic(ctx, key, direction).await?;
            if resp != RespValue::Null {
                return Ok((
                    RespValue::Array(vec![RespValue::BulkString(key.clone()), resp]),
                    outcome,
                ));
            }
        }

        // --- Phase 2: Prepare for blocking if no data was found ---
        let (tx, mut rx) = oneshot::channel();
        let shared_waker = Arc::new(Mutex::new(Some(tx)));
        let waiter_info = WaiterInfo {
            session_id: ctx.session_id,
            waker: shared_waker.clone(),
        };

        // --- CRITICAL SECTION: Register waker BEFORE releasing locks ---
        for key in keys {
            self.waiters
                .entry(key.clone())
                .or_default()
                .push_back(waiter_info.clone());
        }
        debug!(
            "Session {}: Registered to block on keys: {:?}",
            ctx.session_id, keys
        );

        // --- Phase 3: Release locks and enter blocking wait ---
        ctx.release_locks();
        let block_result = self
            .wait_with_polling(keys, &mut rx, wait_timeout, &ctx.state)
            .await;

        // --- Phase 4: Process result and clean up ---
        self.remove_waiter(keys, &shared_waker);

        match block_result {
            BlockerOutcome::TimedOut => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            BlockerOutcome::Moved(slot) => {
                let addr = ctx
                    .state
                    .cluster
                    .as_ref()
                    .unwrap()
                    .get_node_for_slot(slot)
                    .map_or_else(String::new, |node| node.node_info.addr.clone());
                Err(SpinelDBError::Moved { slot, addr })
            }
            BlockerOutcome::Woken(woken_value) => {
                if let WokenValue::List(popped) = woken_value {
                    Ok((
                        RespValue::Array(vec![
                            RespValue::BulkString(popped.key),
                            RespValue::BulkString(popped.value),
                        ]),
                        WriteOutcome::DidNotWrite, // Write was handled by the notifier
                    ))
                } else {
                    Err(SpinelDBError::Internal(
                        "Received wrong woken value type for list pop".into(),
                    ))
                }
            }
        }
    }

    /// The main orchestrator for the `BLMOVE` command.
    pub async fn orchestrate_blmove(
        self: &Arc<Self>,
        ctx: &mut ExecutionContext<'_>,
        source_key: &Bytes,
        dest_key: &Bytes,
        from: Side,
        to: Side,
        wait_timeout: Duration,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // --- Phase 1: Attempt a non-blocking LMOVE ---
        let (resp, outcome) = lmove_logic(source_key, dest_key, from, to, ctx).await?;
        if resp != RespValue::Null {
            return Ok((resp, outcome));
        }

        // --- Phase 2: Prepare for blocking ---
        let (tx, mut rx) = oneshot::channel();
        let shared_waker = Arc::new(Mutex::new(Some(tx)));
        let waiter_info = WaiterInfo {
            session_id: ctx.session_id,
            waker: shared_waker.clone(),
        };

        // --- CRITICAL SECTION: Register waker for the source key ---
        self.waiters
            .entry(source_key.clone())
            .or_default()
            .push_back(waiter_info);
        debug!(
            "Session {}: Registered to block on BLMOVE source key: {}",
            ctx.session_id,
            String::from_utf8_lossy(source_key)
        );

        // --- Phase 3: Release locks and block ---
        ctx.release_locks();
        let block_result = self
            .wait_with_polling(&[source_key.clone()], &mut rx, wait_timeout, &ctx.state)
            .await;

        // --- Phase 4: Process result and clean up ---
        self.remove_waiter(&[source_key.clone()], &shared_waker);

        match block_result {
            BlockerOutcome::TimedOut => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            BlockerOutcome::Moved(slot) => {
                let addr = ctx
                    .state
                    .cluster
                    .as_ref()
                    .unwrap()
                    .get_node_for_slot(slot)
                    .map_or_else(String::new, |node| node.node_info.addr.clone());
                Err(SpinelDBError::Moved { slot, addr })
            }
            BlockerOutcome::Woken(woken_value) => {
                // The value was popped from the source by the notifier.
                // We only need to acquire a lock on the destination and push it.
                if let WokenValue::List(popped) = woken_value {
                    let db = ctx.db; // Use the DB from the original context
                    let push_cmd = Command::RPush(RPush {
                        key: dest_key.clone(),
                        values: vec![popped.value.clone()],
                    });

                    let mut dest_ctx = ExecutionContext {
                        state: ctx.state.clone(),
                        locks: db.determine_locks_for_command(&push_cmd).await,
                        db,
                        command: Some(push_cmd),
                        session_id: ctx.session_id,
                        authenticated_user: ctx.authenticated_user.clone(),
                    };
                    list_push_to_dest_from_move(&mut dest_ctx, dest_key, popped.value, to).await
                } else {
                    Err(SpinelDBError::Internal(
                        "Received wrong woken value type for list move".into(),
                    ))
                }
            }
        }
    }

    /// The main orchestrator for blocking zset pop operations (`BZPOPMIN`, `BZPOPMAX`).
    pub async fn orchestrate_zset_blocking_pop(
        self: &Arc<Self>,
        ctx: &mut ExecutionContext<'_>,
        keys: &[Bytes],
        side: PopSide,
        wait_timeout: Duration,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // --- Phase 1: Attempt a non-blocking pop ---
        for key in keys {
            let (resp, outcome) = zpop_logic(ctx, key, side, Some(1)).await?;
            if let RespValue::Array(arr) = &resp {
                if !arr.is_empty() {
                    let mut final_resp = vec![RespValue::BulkString(key.clone())];
                    final_resp.extend_from_slice(arr);
                    return Ok((RespValue::Array(final_resp), outcome));
                }
            }
        }

        // --- Phase 2: Prepare for blocking ---
        let (tx, mut rx) = oneshot::channel();
        let shared_waker = Arc::new(Mutex::new(Some(tx)));
        let waiter_info = WaiterInfo {
            session_id: ctx.session_id,
            waker: shared_waker.clone(),
        };

        // --- CRITICAL SECTION: Register waker BEFORE releasing locks ---
        for key in keys {
            self.waiters
                .entry(key.clone())
                .or_default()
                .push_back(waiter_info.clone());
        }
        debug!(
            "Session {}: Registered to block on zset keys: {:?}",
            ctx.session_id, keys
        );

        // --- Phase 3: Release locks and block ---
        ctx.release_locks();
        let block_result = self
            .wait_with_polling(keys, &mut rx, wait_timeout, &ctx.state)
            .await;

        // --- Phase 4: Process result and clean up ---
        self.remove_waiter(keys, &shared_waker);

        match block_result {
            BlockerOutcome::TimedOut => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            BlockerOutcome::Moved(slot) => {
                let addr = ctx
                    .state
                    .cluster
                    .as_ref()
                    .unwrap()
                    .get_node_for_slot(slot)
                    .map_or_else(String::new, |node| node.node_info.addr.clone());
                Err(SpinelDBError::Moved { slot, addr })
            }
            BlockerOutcome::Woken(woken_value) => {
                // No re-read necessary. Use the value sent by the notifier.
                if let WokenValue::ZSet(popped) = woken_value {
                    let resp = RespValue::Array(vec![
                        RespValue::BulkString(popped.key),
                        RespValue::BulkString(popped.member),
                        RespValue::BulkString(popped.score.to_string().into()),
                    ]);
                    Ok((resp, WriteOutcome::DidNotWrite)) // Write handled by notifier
                } else {
                    Err(SpinelDBError::Internal(
                        "Received wrong woken value type for zset pop".into(),
                    ))
                }
            }
        }
    }

    /// The actual waiting logic, supporting both cluster and standalone modes.
    async fn wait_with_polling(
        &self,
        keys: &[Bytes],
        rx: &mut oneshot::Receiver<WokenValue>,
        wait_timeout: Duration,
        state: &Arc<ServerState>,
    ) -> BlockerOutcome {
        let Some(cluster_state) = &state.cluster else {
            // Standalone mode: a simple timeout is sufficient.
            return match timeout(wait_timeout, rx).await {
                Ok(Ok(popped)) => BlockerOutcome::Woken(popped),
                _ => BlockerOutcome::TimedOut,
            };
        };

        // Cluster mode: poll periodically to check for slot migrations.
        const POLLING_TIMEOUT: Duration = Duration::from_millis(500);
        let deadline = Instant::now() + wait_timeout;
        let my_slot = get_slot(&keys[0]);

        loop {
            let now = Instant::now();
            if now >= deadline {
                return BlockerOutcome::TimedOut;
            }
            let time_left = deadline - now;
            let current_timeout = POLLING_TIMEOUT.min(time_left);

            match timeout(current_timeout, &mut *rx).await {
                Ok(Ok(popped)) => return BlockerOutcome::Woken(popped),
                Ok(Err(_)) => return BlockerOutcome::TimedOut, // Waker was dropped.
                Err(_) => {
                    // Timeout elapsed. Check if the slot has moved.
                    if !cluster_state.i_own_slot(my_slot) {
                        return BlockerOutcome::Moved(my_slot);
                    }
                }
            }
        }
    }

    /// Called by list write commands (`LPUSH`/`RPUSH`). It attempts to hand off a value
    /// to a waiting client. If successful, the value bypasses the list entirely.
    /// Returns true if a waiter was notified and the value was consumed.
    pub fn notify_and_consume_for_push(&self, key: &Bytes, value: Bytes) -> bool {
        loop {
            let waiter_info = if let Some(mut queue) = self.waiters.get_mut(key) {
                if queue.is_empty() {
                    return false; // No waiters to notify
                } else if queue.front().unwrap().waker.lock().unwrap().is_none() {
                    // Waker has been dropped (e.g., client disconnected), clean it up.
                    queue.pop_front();
                    continue;
                }
                queue.pop_front()
            } else {
                return false;
            };

            if let Some(info) = waiter_info {
                let waker = if let Ok(mut guard) = info.waker.lock() {
                    guard.take()
                } else {
                    None
                };

                if let Some(waker) = waker {
                    let popped_value = PoppedValue {
                        key: key.clone(),
                        value: value.clone(),
                    };
                    if waker.send(WokenValue::List(popped_value)).is_ok() {
                        // The value was successfully sent to the waiter, meaning it was "consumed".
                        // The PUSH command should NOT add this value to the list.
                        tracing::debug!(
                            "Atomically handed off value to a waiter for list key '{}'",
                            String::from_utf8_lossy(key)
                        );
                        return true;
                    }
                }
            } else {
                return false;
            }
        }
    }

    /// Wakes up any clients waiting on a key that is about to be modified or deleted.
    /// This is a simple notification; the waiting client is responsible for re-checking the key state.
    pub fn wake_waiters_for_modification(&self, key: &Bytes) {
        if let Some(mut queue) = self.waiters.get_mut(key) {
            // Wake up everyone waiting on this key.
            while let Some(info) = queue.pop_front() {
                if let Ok(mut guard) = info.waker.lock() {
                    // take() ensures we only send once.
                    if let Some(waker) = guard.take() {
                        // The woken value doesn't matter here, as the client will re-read the state.
                        // We send a value just to unblock the oneshot receiver.
                        let dummy_value = PoppedValue {
                            key: key.clone(),
                            value: Bytes::new(),
                        };
                        let _ = waker.send(WokenValue::List(dummy_value));
                    }
                }
            }
        }
    }

    /// Called by zset write commands (`ZADD`/`ZINCRBY`) to atomically pop an element and notify a waiter.
    pub fn notify_and_pop_zset_waiter(
        &self,
        zset: &mut SortedSet,
        key: &Bytes,
        side: PopSide,
    ) -> bool {
        let popped_entry = match side {
            PopSide::Min => zset.pop_first(),
            PopSide::Max => zset.pop_last(),
        };

        if let Some(popped) = popped_entry {
            loop {
                let waiter_info = if let Some(mut queue) = self.waiters.get_mut(key) {
                    if queue.is_empty() {
                        drop(queue);
                        self.waiters.remove(key);
                        break;
                    }
                    queue.pop_front()
                } else {
                    break;
                };

                if let Some(info) = waiter_info {
                    let waker = if let Ok(mut guard) = info.waker.lock() {
                        guard.take()
                    } else {
                        None
                    };
                    if let Some(waker) = waker {
                        let woken_value = ZSetPoppedValue {
                            key: key.clone(),
                            member: popped.member.clone(),
                            score: popped.score,
                        };
                        if waker.send(WokenValue::ZSet(woken_value)).is_ok() {
                            debug!(
                                "Atomically popped and notified a waiter for zset key '{}'",
                                String::from_utf8_lossy(key)
                            );
                            return true; // Success: value is consumed by the waiter.
                        }
                    }
                } else {
                    break;
                }
            }

            // If we are here, no waiter was notified, so put the element back.
            zset.add(popped.score, popped.member);
        }
        false
    }

    /// Removes a specific waker from all associated key queues.
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

    /// Removes all wakers for a given session_id upon client disconnection.
    pub fn remove_waiters_for_session(&self, session_id: u64) {
        self.waiters.iter_mut().for_each(|mut queue| {
            queue.retain(|info| info.session_id != session_id);
        });
        self.waiters.retain(|_, queue| !queue.is_empty());
        debug!(
            "Removed any pending list/zset blockers for session_id {}.",
            session_id
        );
    }
}

/// A helper function used by `orchestrate_blmove` to push the moved value to the destination list.
async fn list_push_to_dest_from_move<'a>(
    ctx: &mut ExecutionContext<'a>,
    dest_key: &Bytes,
    value: Bytes,
    to: Side,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let (shard, guard) = ctx.get_single_shard_context_mut()?;
    let entry = guard.get_or_insert_with_mut(dest_key.clone(), || {
        StoredValue::new(DataValue::List(VecDeque::new()))
    });

    if let DataValue::List(list) = &mut entry.data {
        let val_len = value.len();
        match to {
            Side::Left => list.push_front(value.clone()),
            Side::Right => list.push_back(value.clone()),
        };
        entry.size += val_len;
        entry.version = entry.version.wrapping_add(1);
        shard.update_memory(val_len as isize);

        ctx.state
            .blocker_manager
            .notify_and_consume_for_push(dest_key, value.clone());

        Ok((
            RespValue::BulkString(value),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    } else {
        Err(SpinelDBError::WrongType)
    }
}
