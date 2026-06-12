// src/core/state/persistence.rs

//! Contains state definitions related to data persistence (AOF/SPLDB).

use crate::core::events::PropagatedWork;
use crate::core::tasks::lazy_free::LazyFreeItem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

/// Holds the state for an in-progress AOF rewrite operation.
#[derive(Debug, Default)]
pub struct AofRewriteState {
    /// True if an AOF rewrite is currently active.
    pub is_in_progress: bool,
    /// Indicates whether the rewrite succeeded. Set by the rewrite task before signaling completion.
    /// `None` while in progress, `Some(true)` on success, `Some(false)` on failure.
    pub succeeded: Option<bool>,
    /// Buffers write commands that arrive while the rewrite is in progress.
    pub buffer: Vec<PropagatedWork>,
    /// The estimated total size of the buffered commands in bytes.
    pub buffer_size: usize,
}

/// Holds all state and channels related to persistence.
#[derive(Debug)]
pub struct PersistenceState {
    /// An atomic flag indicating if a background SPLDB save is in progress.
    pub is_saving_spldb: Arc<AtomicBool>,
    /// The state of the AOF rewrite process, protected by a Mutex.
    pub aof_rewrite_state: Arc<Mutex<AofRewriteState>>,
    /// A handle to the spawned AOF rewrite task, if any.
    pub aof_rewrite_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// A handle to the spawned BGSAVE task, if any.
    pub bgsave_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// A counter for the number of keys dirtied since the last save.
    pub dirty_keys_counter: Arc<AtomicU64>,
    /// The timestamp of the last successful SPLDB save.
    pub last_save_success_time: Arc<Mutex<Option<Instant>>>,
    /// The timestamp of the last failed SPLDB save.
    pub last_save_failure_time: Arc<Mutex<Option<Instant>>>,
    /// A counter for errors when the lazy-free queue is full.
    pub lazy_free_queue_full_errors: AtomicU64,
    /// A channel to signal the AOF writer to perform a periodic fsync.
    pub aof_fsync_request_tx: mpsc::Sender<()>,
    /// A watch channel to notify the AOF writer that a rewrite has completed.
    pub aof_rewrite_complete_tx: tokio::sync::watch::Sender<()>,
    /// A channel to send (key, value) pairs for asynchronous deallocation (UNLINK).
    pub lazy_free_tx: mpsc::Sender<Vec<LazyFreeItem>>,
    /// The size of the AOF file at the end of the last successful rewrite.
    /// Used by the auto-rewrite manager to calculate growth percentage.
    pub aof_last_rewrite_size: Arc<AtomicU64>,
}

impl PersistenceState {
    /// Creates a new `PersistenceState` with initialized channels and counters.
    pub fn new(
        aof_fsync_request_tx: mpsc::Sender<()>,
        aof_rewrite_complete_tx: tokio::sync::watch::Sender<()>,
        lazy_free_tx: mpsc::Sender<Vec<LazyFreeItem>>,
    ) -> Self {
        Self {
            is_saving_spldb: Arc::new(AtomicBool::new(false)),
            aof_rewrite_state: Arc::new(Mutex::new(AofRewriteState::default())),
            aof_rewrite_handle: Arc::new(Mutex::new(None)),
            bgsave_handle: Arc::new(Mutex::new(None)),
            dirty_keys_counter: Arc::new(AtomicU64::new(0)),
            last_save_success_time: Arc::new(Mutex::new(None)),
            last_save_failure_time: Arc::new(Mutex::new(None)),
            lazy_free_queue_full_errors: AtomicU64::new(0),
            aof_fsync_request_tx,
            aof_rewrite_complete_tx,
            lazy_free_tx,
            aof_last_rewrite_size: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Atomically increments the dirty keys counter.
    pub fn increment_dirty_keys(&self, count: u64) {
        self.dirty_keys_counter.fetch_add(count, Ordering::Relaxed);
    }

    /// Atomically increments the counter for lazy-free queue errors.
    pub fn increment_lazy_free_errors(&self) {
        self.lazy_free_queue_full_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Gets the current count of lazy-free queue errors.
    pub fn get_lazy_free_errors(&self) -> u64 {
        self.lazy_free_queue_full_errors.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_persistence_state() -> PersistenceState {
        let (fsync_tx, _fsync_rx) = tokio::sync::mpsc::channel(1);
        let (rewrite_tx, _rewrite_rx) = tokio::sync::watch::channel(());
        let (lazy_free_tx, _lazy_free_rx) = tokio::sync::mpsc::channel(1);
        PersistenceState::new(fsync_tx, rewrite_tx, lazy_free_tx)
    }

    #[test]
    fn test_new_starts_not_saving() {
        let s = make_persistence_state();
        assert!(!s.is_saving_spldb.load(Ordering::Relaxed));
    }

    #[test]
    fn test_new_starts_with_zero_dirty_keys() {
        let s = make_persistence_state();
        assert_eq!(s.dirty_keys_counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_new_starts_with_zero_lazy_free_errors() {
        let s = make_persistence_state();
        assert_eq!(s.get_lazy_free_errors(), 0);
    }

    #[test]
    fn test_increment_dirty_keys() {
        let s = make_persistence_state();
        s.increment_dirty_keys(5);
        assert_eq!(s.dirty_keys_counter.load(Ordering::Relaxed), 5);
        s.increment_dirty_keys(3);
        assert_eq!(s.dirty_keys_counter.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn test_increment_dirty_keys_zero() {
        let s = make_persistence_state();
        s.increment_dirty_keys(0);
        assert_eq!(s.dirty_keys_counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_increment_lazy_free_errors() {
        let s = make_persistence_state();
        s.increment_lazy_free_errors();
        assert_eq!(s.get_lazy_free_errors(), 1);
        s.increment_lazy_free_errors();
        s.increment_lazy_free_errors();
        assert_eq!(s.get_lazy_free_errors(), 3);
    }

    #[tokio::test]
    async fn test_aof_rewrite_state_default() {
        let state = AofRewriteState::default();
        assert!(!state.is_in_progress);
        assert!(state.succeeded.is_none());
        assert!(state.buffer.is_empty());
        assert_eq!(state.buffer_size, 0);
    }

    #[tokio::test]
    async fn test_aof_rewrite_state_buffer_tracks_size() {
        let state = AofRewriteState {
            is_in_progress: true,
            succeeded: None,
            buffer_size: 1024,
            ..Default::default()
        };
        assert!(state.is_in_progress);
        assert_eq!(state.buffer_size, 1024);
    }

    #[test]
    fn test_is_saving_spldb_can_be_set() {
        let s = make_persistence_state();
        s.is_saving_spldb.store(true, Ordering::Relaxed);
        assert!(s.is_saving_spldb.load(Ordering::Relaxed));
        s.is_saving_spldb.store(false, Ordering::Relaxed);
        assert!(!s.is_saving_spldb.load(Ordering::Relaxed));
    }

    #[test]
    fn test_aof_last_rewrite_size_default_zero() {
        let s = make_persistence_state();
        assert_eq!(s.aof_last_rewrite_size.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_aof_last_rewrite_size_can_be_updated() {
        let s = make_persistence_state();
        s.aof_last_rewrite_size.store(4096, Ordering::Relaxed);
        assert_eq!(s.aof_last_rewrite_size.load(Ordering::Relaxed), 4096);
    }
}
