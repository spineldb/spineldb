// src/core/state/cache.rs

//! Contains all state and logic related to the Intelligent Cache feature.

use crate::core::commands::cache::cache_fetch::{CacheFetch, FetchOutcome};
use crate::core::commands::cache::cache_set::CacheSet;
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::metrics;
use crate::core::state::ServerState;
use crate::core::storage::cache_types::{
    CacheBody, CachePolicy, ManifestEntry, ManifestState, VariantMap,
};
use crate::core::storage::db::ExecutionContext;
use crate::core::{Command, SpinelDBError};
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::{BoxFuture, Shared};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{debug, warn};

/// The time window within which a cache variant is considered "hot" or popular,
/// making it a candidate for proactive revalidation.
const CACHE_REVALIDATOR_HOT_VARIANT_WINDOW: Duration = Duration::from_secs(3600); // 1 hour

/// A type alias for a shared, clonable future that handles a single origin fetch.
/// This is the core of the cache stampede protection mechanism.
pub type SharedFetch = Shared<BoxFuture<'static, Result<FetchOutcome, Arc<SpinelDBError>>>>;

/// Represents a job sent to the background revalidation worker.
#[derive(Debug)]
pub struct RevalidationJob {
    pub key: Bytes,
    pub url: String,
    pub variant_hash: u64,
}

/// Holds all state and logic related to the Intelligent Cache feature.
#[derive(Debug)]
pub struct CacheState {
    /// Per-key shared futures to prevent cache stampedes on `CACHE.FETCH`.
    pub fetch_locks: Arc<DashMap<Bytes, SharedFetch>>,
    /// Per-key locks to prevent stampedes on stale-while-revalidate (SWR) background fetches.
    pub swr_locks: Arc<DashMap<Bytes, Arc<Mutex<()>>>>,
    /// Counter for cache hits.
    pub hits: AtomicU64,
    /// Counter for cache misses.
    pub misses: AtomicU64,
    /// Counter for stale cache hits (served during SWR or Grace periods).
    pub stale_hits: AtomicU64,
    /// Counter for successful revalidations.
    pub revalidations: AtomicU64,
    /// Counter for cache keys evicted due to memory pressure.
    pub evictions: AtomicU64,
    /// Stores user-defined caching rules for declarative caching.
    pub policies: RwLock<Vec<CachePolicy>>,
    /// A set of keys that match a `prewarm` policy.
    /// This allows the revalidator to efficiently sample only relevant keys.
    pub prewarm_keys: RwLock<HashSet<Bytes>>,
    /// A channel to send revalidation jobs to the dedicated worker.
    pub revalidation_tx: mpsc::Sender<RevalidationJob>,
    /// Stores the last known purge epoch for a given tag. This is a logical clock
    /// used to invalidate tagged content in a cluster without relying on synchronized time.
    pub tag_purge_epochs: Arc<DashMap<Bytes, u64>>,
    /// Stores patterns for lazy background purging via `CACHE.PURGE`.
    /// Key: Glob pattern, Value: Time the purge was requested.
    pub purge_patterns: Arc<DashMap<Bytes, Instant>>,
    /// Manually applied locks from `CACHE.LOCK`.
    /// Key: Cache key, Value: Expiry time of the lock.
    pub manual_locks: Arc<DashMap<Bytes, Instant>>,
    /// A synchronized writer for the on-disk cache manifest file.
    pub manifest_writer: Arc<Mutex<Option<BufWriter<TokioFile>>>>,
}

impl CacheState {
    /// Creates a new `CacheState` with initialized counters and maps.
    pub fn new(revalidation_tx: mpsc::Sender<RevalidationJob>) -> Self {
        Self {
            fetch_locks: Arc::new(DashMap::with_capacity(256)),
            swr_locks: Arc::new(DashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            stale_hits: AtomicU64::new(0),
            revalidations: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            policies: RwLock::new(Vec::new()),
            prewarm_keys: RwLock::new(HashSet::new()),
            revalidation_tx,
            tag_purge_epochs: Arc::new(DashMap::new()),
            purge_patterns: Arc::new(DashMap::new()),
            manual_locks: Arc::new(DashMap::new()),
            manifest_writer: Arc::new(Mutex::new(None)),
        }
    }

    /// Logs an entry to the on-disk cache manifest file.
    pub async fn log_manifest(
        &self,
        key: Bytes,
        state: ManifestState,
        path: PathBuf,
    ) -> Result<(), SpinelDBError> {
        let mut writer_guard = self.manifest_writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            let entry = ManifestEntry {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                state,
                path,
                key,
            };
            let mut line = serde_json::to_vec(&entry)?;
            line.push(b'\n');
            writer.write_all(&line).await?;
            writer.flush().await?;
        }
        Ok(())
    }

    /// Atomically increments the counter for cache hits.
    pub fn increment_hits(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        metrics::CACHE_HITS_TOTAL.inc();
    }

    /// Atomically increments the counter for cache misses.
    pub fn increment_misses(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        metrics::CACHE_MISSES_TOTAL.inc();
    }

    /// Atomically increments the counter for stale cache hits.
    pub fn increment_stale_hits(&self) {
        self.stale_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increments the counter for successful revalidations.
    pub fn increment_revalidations(&self) {
        self.revalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increments the counter for cache evictions.
    pub fn increment_evictions(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        metrics::CACHE_EVICTIONS_TOTAL.inc();
    }

    /// Performs an HTTP fetch to an origin server and updates the cache.
    /// This is a utility function used by background revalidation tasks.
    pub async fn fetch_from_origin(
        &self,
        server_state: &Arc<ServerState>,
        cmd: &CacheFetch,
        bypass_store: bool,
    ) -> Result<(Bytes, WriteOutcome), SpinelDBError> {
        self.increment_misses();
        let client = reqwest::Client::new();
        let res =
            client.get(&cmd.url).send().await.map_err(|e| {
                SpinelDBError::Internal(format!("Failed to fetch from origin: {e}"))
            })?;

        if res.status() != reqwest::StatusCode::OK {
            return Err(SpinelDBError::Internal(format!(
                "Origin server responded with status {}",
                res.status()
            )));
        }

        let headers = res.headers().clone();
        let body = res
            .bytes()
            .await
            .map_err(|e| SpinelDBError::Internal(format!("Failed to read response body: {e}")))?;

        if headers
            .get(reqwest::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.contains("private"))
        {
            debug!("Origin responded with 'Cache-Control: private'. Bypassing cache store.");
            return Ok((body, WriteOutcome::DidNotWrite));
        }

        if bypass_store {
            return Ok((body, WriteOutcome::DidNotWrite));
        }

        let set_cmd = CacheSet {
            key: cmd.key.clone(),
            body_data: body.clone(),
            ttl: cmd.ttl,
            swr: cmd.swr,
            grace: cmd.grace,
            revalidate_url: Some(cmd.url.clone()),
            etag: headers
                .get(reqwest::header::ETAG)
                .map(|v| Bytes::from(v.as_bytes().to_vec())),
            last_modified: headers
                .get(reqwest::header::LAST_MODIFIED)
                .map(|v| Bytes::from(v.as_bytes().to_vec())),
            tags: cmd.tags.clone(),
            vary: cmd.vary.clone(),
            headers: cmd.headers.clone(),
        };

        let db = server_state.get_db(0).unwrap();
        let set_cmd_internal = set_cmd.clone();

        let set_command_for_lock = Command::Cache(crate::core::commands::cache::Cache {
            subcommand: crate::core::commands::cache::command::CacheSubcommand::Set(set_cmd),
        });

        let mut set_ctx = ExecutionContext {
            state: server_state.clone(),
            locks: db.determine_locks_for_command(&set_command_for_lock).await,
            db: &db,
            command: Some(set_command_for_lock),
            session_id: 0,
            authenticated_user: None,
        };
        let (_, write_outcome) = set_cmd_internal
            .execute_internal(&mut set_ctx, CacheBody::InMemory(body.clone()))
            .await?;

        Ok((body, write_outcome))
    }

    /// Queues jobs for background, asynchronous revalidation for a cache key.
    /// This smart version only queues jobs for variants that have been accessed recently.
    pub async fn trigger_smart_background_revalidation(&self, key: Bytes, variants: VariantMap) {
        let now = Instant::now();
        let jobs_to_queue: Vec<_> = variants
            .into_iter()
            .filter_map(|(hash, variant)| {
                if now.saturating_duration_since(variant.last_accessed)
                    <= CACHE_REVALIDATOR_HOT_VARIANT_WINDOW
                {
                    variant
                        .metadata
                        .revalidate_url
                        .clone()
                        .map(|url| RevalidationJob {
                            key: key.clone(),
                            url,
                            variant_hash: hash,
                        })
                } else {
                    None
                }
            })
            .collect();

        if !jobs_to_queue.is_empty() {
            debug!(
                "Queueing {} SMART revalidation jobs for key '{}'",
                jobs_to_queue.len(),
                String::from_utf8_lossy(&key)
            );

            for job in jobs_to_queue {
                if let Err(e) = self.revalidation_tx.try_send(job) {
                    warn!(
                        "Failed to queue cache revalidation job, worker may be busy: {}",
                        e
                    );
                }
            }
        }
    }
}
