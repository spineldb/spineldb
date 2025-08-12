// src/core/blocking.rs

//! Manages clients that are blocked waiting for data on list/zset keys.
//! This implementation is cluster-aware and handles slot migrations safely.

use crate::core::cluster::slot::get_slot;
use crate::core::commands::command_trait::{CommandExt, WriteOutcome};
use crate::core::commands::list::lmove::{Side, lmove_logic};
use crate::core::commands::list::logic::list_pop_logic;
use crate::core::commands::zset::zpop_logic::{PopSide, zpop_logic};
use crate::core::state::ServerState;
use crate::core::storage::db::zset::SortedSet;
use crate::core::storage::db::{ExecutionContext, PopDirection};
use crate::core::{Command, RespValue, SpinelDBError};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{debug, error, warn};

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
            .wait_with_polling(
                std::slice::from_ref(source_key),
                &mut rx,
                wait_timeout,
                &ctx.state,
            )
            .await;

        // --- Phase 4: Process result and clean up ---
        self.remove_waiter(std::slice::from_ref(source_key), &shared_waker);

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
                    let db = ctx.db;
                    let push_cmd = match to {
                        Side::Left => Command::LPush(crate::core::commands::list::LPush {
                            key: dest_key.clone(),
                            values: vec![popped.value.clone()],
                        }),
                        Side::Right => Command::RPush(crate::core::commands::list::RPush {
                            key: dest_key.clone(),
                            values: vec![popped.value.clone()],
                        }),
                    };

                    let mut dest_ctx = ExecutionContext {
                        state: ctx.state.clone(),
                        locks: db.determine_locks_for_command(&push_cmd).await,
                        db,
                        command: Some(push_cmd.clone()),
                        session_id: ctx.session_id,
                        authenticated_user: ctx.authenticated_user.clone(),
                    };

                    let push_result = push_cmd.execute(&mut dest_ctx).await;

                    if let Err(e) = push_result {
                        warn!(
                            "Failed to push element to destination in BLMOVE (key: '{}', error: {}). \
                             Attempting to return element to source key '{}'.",
                            String::from_utf8_lossy(dest_key),
                            e,
                            String::from_utf8_lossy(source_key)
                        );

                        let return_push_cmd = match from {
                            Side::Left => Command::LPush(crate::core::commands::list::LPush {
                                key: source_key.clone(),
                                values: vec![popped.value.clone()],
                            }),
                            Side::Right => Command::RPush(crate::core::commands::list::RPush {
                                key: source_key.clone(),
                                values: vec![popped.value.clone()],
                            }),
                        };

                        let mut source_ctx = ExecutionContext {
                            state: ctx.state.clone(),
                            locks: db.determine_locks_for_command(&return_push_cmd).await,
                            db,
                            command: Some(return_push_cmd.clone()),
                            session_id: ctx.session_id,
                            authenticated_user: ctx.authenticated_user.clone(),
                        };

                        if let Err(return_err) = return_push_cmd.execute(&mut source_ctx).await {
                            error!(
                                "CRITICAL DATA LOSS: Failed to return element '{}' back to source list '{}' after BLMOVE failure. Error: {}",
                                String::from_utf8_lossy(&popped.value),
                                String::from_utf8_lossy(source_key),
                                return_err
                            );
                        }

                        return Err(e);
                    }

                    Ok((
                        RespValue::BulkString(popped.value),
                        WriteOutcome::Write { keys_modified: 2 },
                    ))
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
            if let RespValue::Array(arr) = &resp
                && !arr.is_empty()
            {
                let mut final_resp = vec![RespValue::BulkString(key.clone())];
                final_resp.extend_from_slice(arr);
                return Ok((RespValue::Array(final_resp), outcome));
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
                if let WokenValue::ZSet(popped) = woken_value {
                    let resp = RespValue::Array(vec![
                        RespValue::BulkString(popped.key),
                        RespValue::BulkString(popped.member),
                        RespValue::BulkString(popped.score.to_string().into()),
                    ]);
                    Ok((resp, WriteOutcome::DidNotWrite))
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
            return match timeout(wait_timeout, rx).await {
                Ok(Ok(popped)) => BlockerOutcome::Woken(popped),
                _ => BlockerOutcome::TimedOut,
            };
        };

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
                    if !cluster_state.i_own_slot(my_slot) {
                        return BlockerOutcome::Moved(my_slot);
                    }
                }
            }
        }
    }

    /// Called by list write commands (`LPUSH`/`RPUSH`). It attempts to hand off values
    /// to waiting clients. If successful, the value bypasses the list entirely.
    /// Returns the new list length if a waiter was notified and the value was consumed.
    pub fn notify_and_consume_for_push(&self, key: &Bytes, values: &[Bytes]) -> Option<usize> {
        loop {
            let waiter_info = if let Some(mut queue) = self.waiters.get_mut(key) {
                if queue.is_empty() {
                    return None; // No one is waiting.
                } else if queue.front().unwrap().waker.lock().unwrap().is_none() {
                    // Waker has already been used; clean it up and retry.
                    queue.pop_front();
                    continue;
                }
                queue.pop_front()
            } else {
                return None; // No one is waiting for this key.
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
                        value: values[0].clone(),
                    };
                    if waker.send(WokenValue::List(popped_value)).is_ok() {
                        tracing::debug!(
                            "Atomically handed off value to a waiter for list key '{}'",
                            String::from_utf8_lossy(key)
                        );
                        // The first value was consumed. Return the number of remaining values
                        // that will be added to the list (which is the new length for an empty list).
                        return Some(values.len() - 1);
                    }
                }
            } else {
                return None;
            }
        }
    }

    /// Wakes up any clients waiting on a key that is about to be modified or deleted.
    pub fn wake_waiters_for_modification(&self, key: &Bytes) {
        if let Some(mut queue) = self.waiters.get_mut(key) {
            while let Some(info) = queue.pop_front() {
                if let Ok(mut guard) = info.waker.lock()
                    && let Some(waker) = guard.take()
                {
                    let dummy_value = PoppedValue {
                        key: key.clone(),
                        value: Bytes::new(),
                    };
                    let _ = waker.send(WokenValue::List(dummy_value));
                }
            }
        }
    }

    /// Called by zset write commands (`ZADD`/`ZINCRBY`) to atomically pop an element and notify a waiter.
    /// Returns the side that was popped (Min or Max) if a waiter was successfully notified.
    pub fn notify_and_pop_zset_waiter(
        &self,
        zset: &mut SortedSet,
        key: &Bytes,
        side: PopSide,
    ) -> Option<PopSide> {
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
                            // Return the side that was successfully popped and handed off.
                            return Some(side);
                        }
                    }
                } else {
                    break;
                }
            }
            // If no waiter was found or notified, put the popped element back.
            zset.add(popped.score, popped.member);
        }
        // No waiter was notified.
        None
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
