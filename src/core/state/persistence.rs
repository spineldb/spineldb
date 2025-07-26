// src/core/state/persistence.rs

//! Contains state definitions related to data persistence (AOF/SPLDB).

use crate::core::events::PropagatedWork;
use crate::core::storage::data_types::StoredValue;
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
    /// Buffers write commands that arrive while the rewrite is in progress.
    pub buffer: Vec<PropagatedWork>,
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
    /// A channel to send values for asynchronous deallocation (UNLINK).
    pub lazy_free_tx: mpsc::Sender<Vec<StoredValue>>,
    /// The size of the AOF file at the end of the last successful rewrite.
    /// Used by the auto-rewrite manager to calculate growth percentage.
    pub aof_last_rewrite_size: Arc<AtomicU64>,
}

impl PersistenceState {
    /// Creates a new `PersistenceState` with initialized channels and counters.
    pub fn new(
        aof_fsync_request_tx: mpsc::Sender<()>,
        aof_rewrite_complete_tx: tokio::sync::watch::Sender<()>,
        lazy_free_tx: mpsc::Sender<Vec<StoredValue>>,
    ) -> Self {
        Self {
            is_saving_spldb: Arc::new(AtomicBool::new(false)),
            aof_rewrite_state: Arc::new(Mutex::new(AofRewriteState::default())),
            aof_rewrite_handle: Arc::new(Mutex::new(None)),
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
