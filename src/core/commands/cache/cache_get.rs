// src/core/commands/cache/cache_get.rs

//! Implements the `CACHE.GET` command, which retrieves a cached object.
//! This implementation supports content variants via the `Vary` header,
//! and advanced stale content serving strategies like stale-while-revalidate.

use super::helpers::calculate_variant_hash;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_bytes};
use crate::core::handler::command_router::RouteResponse;
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::storage::cache_types::CacheBody;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, IF_MODIFIED_SINCE, IF_NONE_MATCH};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File as TokioFile;
use tokio::io::copy as tokio_copy;
use tokio::sync::MutexGuard;
use tracing::{debug, warn};

/// Represents the `CACHE.GET` command with its parsed options.
#[derive(Debug, Clone, Default)]
pub struct CacheGet {
    pub key: Bytes,
    pub revalidate_url: Option<String>,
    pub headers: Option<Vec<(Bytes, Bytes)>>,
    pub if_none_match: Option<Bytes>,
    pub if_modified_since: Option<Bytes>,
    pub force_revalidate: bool,
}

impl ParseCommand for CacheGet {
    /// Parses the `CACHE.GET` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CACHE.GET".to_string()));
        }

        let mut cmd = CacheGet {
            key: extract_bytes(&args[0])?,
            ..Default::default()
        };
        let mut parser = ArgParser::new(&args[1..]);

        if let Some(url) = parser.match_option("revalidate")? {
            cmd.revalidate_url = Some(url);
        }
        if let Some(etag) = parser.match_option::<String>("if-none-match")? {
            cmd.if_none_match = Some(Bytes::from(etag));
        }
        if let Some(date) = parser.match_option::<String>("if-modified-since")? {
            cmd.if_modified_since = Some(Bytes::from(date));
        }
        if parser.match_flag("force-revalidate") {
            cmd.force_revalidate = true;
        }

        if parser.match_flag("headers") {
            let remaining = parser.remaining_args();
            if remaining.len() % 2 != 0 {
                return Err(SpinelDBError::WrongArgumentCount(
                    "CACHE.GET HEADERS".to_string(),
                ));
            }
            cmd.headers = Some(
                remaining
                    .chunks_exact(2)
                    .map(|c| (extract_bytes(&c[0]).unwrap(), extract_bytes(&c[1]).unwrap()))
                    .collect(),
            );
        }
        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for CacheGet {
    /// Executes `CACHE.GET`. This is a thin wrapper around `execute_and_stream`
    /// for compatibility with the command trait. It buffers streaming responses,
    /// while the router can handle them directly for optimal performance.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match self.execute_and_stream(ctx).await? {
            RouteResponse::Single(val) => Ok((val, WriteOutcome::DidNotWrite)),
            RouteResponse::NoOp => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            RouteResponse::StreamBody { mut file, .. } => {
                let mut body = Vec::new();
                tokio_copy(&mut file, &mut body).await?;
                Ok((
                    RespValue::BulkString(body.into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            _ => Err(SpinelDBError::Internal(
                "Unexpected response from stream-aware GET logic".into(),
            )),
        }
    }
}

impl CacheGet {
    /// The core execution logic for `CACHE.GET` that can produce a streaming response.
    pub async fn execute_and_stream<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<RouteResponse, SpinelDBError> {
        let state = ctx.state.clone();
        let (_shard, guard) = ctx.get_single_shard_context_mut()?;

        // --- Force Revalidation Path ---
        if self.force_revalidate {
            let Some(entry) = guard.peek(&self.key) else {
                state.cache.increment_misses();
                return Ok(RouteResponse::NoOp);
            };
            let DataValue::HttpCache {
                variants, vary_on, ..
            } = &entry.data
            else {
                return Err(SpinelDBError::WrongType);
            };
            let variant_hash = calculate_variant_hash(vary_on, &self.headers);
            let Some(variant) = variants.get(&variant_hash) else {
                state.cache.increment_misses();
                return Ok(RouteResponse::NoOp);
            };
            let url = self
                .revalidate_url
                .clone()
                .or_else(|| variant.metadata.revalidate_url.clone())
                .ok_or_else(|| {
                    SpinelDBError::InvalidState(
                        "FORCE-REVALIDATE requires a revalidation URL".into(),
                    )
                })?;

            let reval_result = revalidate_and_update_cache(
                state.clone(),
                self.key.clone(),
                url,
                variant_hash,
                self.headers.clone(),
                guard,
            )
            .await;

            // Re-acquire the entry after a potential modification.
            let entry = guard.peek(&self.key).ok_or(SpinelDBError::KeyNotFound)?;
            let DataValue::HttpCache { variants, .. } = &entry.data else {
                return Err(SpinelDBError::WrongType);
            };
            let variant = variants.get(&variant_hash).ok_or_else(|| {
                SpinelDBError::Internal("Variant vanished after revalidation".into())
            })?;

            return match reval_result {
                Ok(Some(new_body)) => Self::create_body_response(&new_body).await,
                Ok(None) => Self::create_body_response(&variant.body).await,
                Err(e) => Err(e),
            };
        }

        // --- Standard Path (not force-revalidate) ---
        let is_valid_entry = {
            let Some(entry) = guard.peek(&self.key) else {
                state.cache.increment_misses();
                return Ok(RouteResponse::NoOp);
            };
            if entry.is_expired() {
                false
            } else {
                let DataValue::HttpCache { tags_epoch, .. } = &entry.data else {
                    return Err(SpinelDBError::WrongType);
                };
                if state.cluster.is_some() {
                    let tags: Vec<Bytes> = guard.get_tags_for_key(&self.key);
                    for tag in tags {
                        if let Some(purge_epoch_entry) = state.cache.tag_purge_epochs.get(&tag) {
                            if *tags_epoch < *purge_epoch_entry.value() {
                                debug!(
                                    "Stale cache entry '{}' due to purged tag '{}'. Deleting.",
                                    String::from_utf8_lossy(&self.key),
                                    String::from_utf8_lossy(&tag)
                                );
                                return Ok(RouteResponse::NoOp);
                            }
                        }
                    }
                }
                true
            }
        };

        if !is_valid_entry {
            guard.pop(&self.key);
            state.cache.increment_misses();
            return Ok(RouteResponse::NoOp);
        }

        let now = Instant::now();
        let entry_expiry = guard.peek(&self.key).unwrap().expiry;
        let entry_swr_expiry = guard.peek(&self.key).unwrap().stale_revalidate_expiry;
        let entry_grace_expiry = guard.peek(&self.key).unwrap().grace_expiry;

        // State 1: Fresh content
        if entry_expiry.is_some_and(|exp| exp > now) {
            let entry = guard.get_mut(&self.key).unwrap();
            let DataValue::HttpCache {
                variants, vary_on, ..
            } = &mut entry.data
            else {
                return Err(SpinelDBError::WrongType);
            };
            let variant_hash = calculate_variant_hash(vary_on, &self.headers);
            let Some(variant) = variants.get_mut(&variant_hash) else {
                state.cache.increment_misses();
                return Ok(RouteResponse::NoOp);
            };
            variant.last_accessed = Instant::now();

            if variant.metadata.etag.as_deref() == Some(b"__NEGATIVE_CACHE__") {
                if let CacheBody::InMemory(body_bytes) = &variant.body {
                    if let Ok(status_str) = std::str::from_utf8(body_bytes) {
                        if let Ok(status_code) = status_str.parse::<u16>() {
                            debug!(
                                "Serving negative cache entry for key '{}'",
                                String::from_utf8_lossy(&self.key)
                            );
                            return Err(SpinelDBError::Internal(format!(
                                "Origin server responded with status {status_code}"
                            )));
                        }
                    }
                }
                return Err(SpinelDBError::Internal(
                    "Failed to fetch from origin (cached failure)".into(),
                ));
            }

            if let Some(req_etag) = &self.if_none_match {
                if variant.metadata.etag.as_ref() == Some(req_etag) {
                    return Ok(RouteResponse::NoOp);
                }
            }
            if let Some(req_ims) = &self.if_modified_since {
                if variant.metadata.last_modified.as_ref() == Some(req_ims) {
                    return Ok(RouteResponse::NoOp);
                }
            }

            state.cache.increment_hits();
            return Self::create_body_response(&variant.body).await;
        }

        // State 2: Stale, but in SWR window
        if entry_swr_expiry.is_some_and(|exp| exp > now) {
            state.cache.increment_stale_hits();
            let entry = guard.get_mut(&self.key).unwrap();
            let DataValue::HttpCache {
                variants, vary_on, ..
            } = &mut entry.data
            else {
                return Err(SpinelDBError::WrongType);
            };
            let variant_hash = calculate_variant_hash(vary_on, &self.headers);
            let Some(variant) = variants.get_mut(&variant_hash) else {
                state.cache.increment_misses();
                return Ok(RouteResponse::NoOp);
            };
            let revalidate_url_from_cache = variant.metadata.revalidate_url.clone();
            variant.last_accessed = Instant::now();

            if let Some(url) = self.revalidate_url.clone().or(revalidate_url_from_cache) {
                let swr_lock = state
                    .cache
                    .swr_locks
                    .entry(self.key.clone())
                    .or_default()
                    .clone();
                if swr_lock.try_lock().is_ok() {
                    debug!(
                        "Acquired SWR lock for key '{}'. Spawning background revalidation.",
                        String::from_utf8_lossy(&self.key)
                    );
                    let state_clone = state.clone();
                    let key_clone = self.key.clone();
                    let headers_clone = self.headers.clone();
                    tokio::spawn(async move {
                        let db = state_clone.get_db(0).unwrap();
                        let shard_index = db.get_shard_index(&key_clone);
                        let mut task_guard = db.get_shard(shard_index).entries.lock().await;
                        if let Err(e) = revalidate_and_update_cache(
                            state_clone,
                            key_clone,
                            url,
                            variant_hash,
                            headers_clone,
                            &mut task_guard,
                        )
                        .await
                        {
                            warn!("Background cache revalidation failed: {}", e);
                        }
                    });
                } else {
                    debug!(
                        "SWR lock for key '{}' is already held. Skipping revalidation.",
                        String::from_utf8_lossy(&self.key)
                    );
                }
            }
            return Self::create_body_response(&variant.body).await;
        }

        // State 3: Stale and past SWR, but in grace window OR explicit revalidate requested
        if self.revalidate_url.is_some() || entry_grace_expiry.is_some_and(|exp| exp > now) {
            let (revalidate_url_from_cache, variant_hash) = {
                let entry = guard.peek(&self.key).unwrap();
                let DataValue::HttpCache {
                    variants, vary_on, ..
                } = &entry.data
                else {
                    return Err(SpinelDBError::WrongType);
                };
                let variant_hash = calculate_variant_hash(vary_on, &self.headers);
                let variant = variants.get(&variant_hash);
                (
                    variant.and_then(|v| v.metadata.revalidate_url.clone()),
                    variant_hash,
                )
            };

            let url = self
                .revalidate_url
                .clone()
                .or(revalidate_url_from_cache)
                .ok_or_else(|| {
                    SpinelDBError::InvalidState(
                        "REVALIDATE-URL is required to serve content in its grace period".into(),
                    )
                })?;
            let reval_result = revalidate_and_update_cache(
                state.clone(),
                self.key.clone(),
                url,
                variant_hash,
                self.headers.clone(),
                guard,
            )
            .await;

            let entry = guard.peek(&self.key).ok_or(SpinelDBError::KeyNotFound)?;
            let DataValue::HttpCache { variants, .. } = &entry.data else {
                return Err(SpinelDBError::WrongType);
            };
            let variant = variants.get(&variant_hash).ok_or_else(|| {
                SpinelDBError::Internal("Variant vanished after revalidation".into())
            })?;

            match reval_result {
                Ok(Some(new_body)) => return Self::create_body_response(&new_body).await,
                Ok(None) => return Self::create_body_response(&variant.body).await,
                Err(_) => {
                    if entry_grace_expiry.is_some_and(|exp| exp > now) {
                        state.cache.increment_stale_hits();
                        return Self::create_body_response(&variant.body).await;
                    }
                }
            };
        }

        // State 4: Expired completely (past grace period)
        state.cache.increment_misses();
        guard.pop(&self.key);
        Ok(RouteResponse::NoOp)
    }

    /// Helper to create the correct RouteResponse based on body location.
    async fn create_body_response(body: &CacheBody) -> Result<RouteResponse, SpinelDBError> {
        match body {
            CacheBody::InMemory(bytes) => {
                Ok(RouteResponse::Single(RespValue::BulkString(bytes.clone())))
            }
            CacheBody::OnDisk { path, size } => {
                let file = TokioFile::open(path).await.map_err(|e| {
                    SpinelDBError::Internal(format!("Failed to open cache file: {e}"))
                })?;
                let resp_header = format!("${size}\r\n").into_bytes();
                Ok(RouteResponse::StreamBody { resp_header, file })
            }
        }
    }
}

/// Performs a conditional HTTP GET to revalidate a cache entry and updates it in place.
/// It takes a mutable reference to the shard guard to avoid a deadlock.
pub(crate) async fn revalidate_and_update_cache<'a>(
    state: Arc<ServerState>,
    key: Bytes,
    url: String,
    variant_hash: u64,
    req_headers: Option<Vec<(Bytes, Bytes)>>,
    guard: &mut MutexGuard<'a, crate::core::storage::db::ShardCache>,
) -> Result<Option<CacheBody>, SpinelDBError> {
    state.cache.increment_revalidations();
    debug!(
        "Revalidating cache for key '{}' (variant {}) from URL '{}'",
        String::from_utf8_lossy(&key),
        variant_hash,
        url
    );

    let Some(entry) = guard.get_mut(&key) else {
        return Err(SpinelDBError::KeyNotFound);
    };
    let DataValue::HttpCache { variants, .. } = &mut entry.data else {
        return Err(SpinelDBError::WrongType);
    };
    let Some(variant) = variants.get_mut(&variant_hash) else {
        return Err(SpinelDBError::Internal(
            "Cache variant disappeared during revalidation".into(),
        ));
    };

    let client = reqwest::Client::new();
    let mut http_headers = HeaderMap::new();
    if let Some(etag) = &variant.metadata.etag {
        if let Ok(h) = HeaderValue::from_bytes(etag) {
            http_headers.insert(IF_NONE_MATCH, h);
        }
    }
    if let Some(lm) = &variant.metadata.last_modified {
        if let Ok(h) = HeaderValue::from_bytes(lm) {
            http_headers.insert(IF_MODIFIED_SINCE, h);
        }
    }
    if let Some(hdrs) = &req_headers {
        for (k, v) in hdrs {
            if let Ok(key) = HeaderName::from_bytes(k) {
                if let Ok(val) = HeaderValue::from_bytes(v) {
                    http_headers.insert(key, val);
                }
            }
        }
    }

    let res = client.get(&url).headers(http_headers).send().await;

    let res = match res {
        Ok(r) => r,
        Err(e) => {
            warn!(
                "Origin fetch failed during revalidation for key '{}': {}",
                String::from_utf8_lossy(&key),
                e
            );
            let now = Instant::now();
            if let Some(grace_exp) = entry.grace_expiry {
                if grace_exp > now {
                    entry.stale_revalidate_expiry = Some(now + Duration::from_secs(10));
                }
            }
            return Err(SpinelDBError::HttpClientError(e.to_string()));
        }
    };

    let status = res.status();
    let res_headers = res.headers().clone();

    if status == reqwest::StatusCode::NOT_MODIFIED {
        state.cache.increment_hits();
        update_ttls_from_headers(entry, &res_headers);
        entry.version += 1;
        return Ok(None);
    }

    if status == reqwest::StatusCode::OK {
        let new_body_bytes = res.bytes().await?;
        let new_body = CacheBody::InMemory(new_body_bytes);

        variant.body = new_body.clone();
        variant.metadata.etag = res_headers
            .get(reqwest::header::ETAG)
            .map(|v| Bytes::from(v.as_bytes().to_vec()));
        variant.metadata.last_modified = res_headers
            .get(reqwest::header::LAST_MODIFIED)
            .map(|v| Bytes::from(v.as_bytes().to_vec()));

        update_ttls_from_headers(entry, &res_headers);
        entry.size = entry.data.memory_usage();
        entry.version += 1;

        return Ok(Some(new_body));
    }

    warn!(
        "Origin responded with unexpected status during revalidation: {}",
        status
    );
    let now = Instant::now();
    if let Some(grace_exp) = entry.grace_expiry {
        if grace_exp > now {
            entry.stale_revalidate_expiry = Some(now + Duration::from_secs(10));
        }
    }
    Err(SpinelDBError::Internal(format!(
        "Origin responded with unexpected status: {status}"
    )))
}

/// Parses Cache-Control headers from the origin's response to update TTLs.
fn update_ttls_from_headers(entry: &mut StoredValue, headers: &HeaderMap) {
    let now = Instant::now();
    let mut fresh_duration_secs = 0;

    if let Some(cc_header) = headers
        .get(reqwest::header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
    {
        for part in cc_header.split(',') {
            if let Some(val_str) = part.trim().strip_prefix("max-age=") {
                if let Ok(secs) = val_str.parse::<u64>() {
                    fresh_duration_secs = secs;
                }
            }
        }
    }

    if fresh_duration_secs > 0 {
        let fresh_duration = Duration::from_secs(fresh_duration_secs);
        let old_swr = entry
            .stale_revalidate_expiry
            .zip(entry.expiry)
            .and_then(|(swr, exp)| swr.checked_duration_since(exp));
        let old_grace = entry
            .grace_expiry
            .zip(entry.stale_revalidate_expiry)
            .and_then(|(grace, swr)| grace.checked_duration_since(swr));

        entry.expiry = Some(now + fresh_duration);
        if let Some(swr) = old_swr {
            entry.stale_revalidate_expiry = Some(now + fresh_duration + swr);
        }
        if let (Some(swr), Some(grace)) = (old_swr, old_grace) {
            entry.grace_expiry = Some(now + fresh_duration + swr + grace);
        }
    }
}

impl CommandSpec for CacheGet {
    fn name(&self) -> &'static str {
        "cache.get"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        // Can result in a write during revalidation, but is mostly a read operation.
        // We mark it as WRITE to ensure correct propagation behavior if revalidation happens.
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.key.clone()];
        if let Some(url) = &self.revalidate_url {
            args.extend([Bytes::from_static(b"REVALIDATE"), url.clone().into()]);
        }
        if let Some(etag) = &self.if_none_match {
            args.extend([Bytes::from_static(b"IF-NONE-MATCH"), etag.clone()]);
        }
        if let Some(ims) = &self.if_modified_since {
            args.extend([Bytes::from_static(b"IF-MODIFIED-SINCE"), ims.clone()]);
        }
        if self.force_revalidate {
            args.push(Bytes::from_static(b"FORCE-REVALIDATE"));
        }
        if let Some(h) = &self.headers {
            args.push(Bytes::from_static(b"HEADERS"));
            args.extend(h.iter().flat_map(|(k, v)| vec![k.clone(), v.clone()]));
        }
        args
    }
}
