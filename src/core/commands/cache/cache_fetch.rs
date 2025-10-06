// src/core/commands/cache/cache_fetch.rs

//! Implements the `CACHE.FETCH` command, providing atomic, stampede-protected
//! fetching of cacheable content from an origin server, with support for streaming large bodies.

use crate::core::commands::cache::cache_get::CacheGet;
use crate::core::commands::cache::cache_set::CacheSet;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_bytes, validate_fetch_url};
use crate::core::handler::command_router::RouteResponse;
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::storage::cache_types::{CacheBody, ManifestState};
use crate::core::storage::db::ExecutionContext;
use crate::core::{Command, RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File as TokioFile, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};
use uuid::Uuid;
use wildmatch::WildMatch;

/// The cloneable result of a shared origin fetch operation.
#[derive(Debug, Clone)]
pub enum FetchOutcome {
    /// The response body is small and held in memory.
    InMemory(Bytes),
    /// The response body was large and has been streamed to a file on disk.
    OnDisk { path: PathBuf, size: u64 },
    /// The origin responded with a non-200 status, which has been negatively cached.
    Negative { status: u16, body: Option<Bytes> },
}

/// Represents the `CACHE.FETCH` command with its parsed arguments.
#[derive(Debug, Clone, Default)]
pub struct CacheFetch {
    pub key: Bytes,
    pub url: String,
    pub ttl: Option<u64>,
    pub swr: Option<u64>,
    pub grace: Option<u64>,
    pub tags: Vec<Bytes>,
    pub vary: Option<Bytes>,
    pub headers: Option<Vec<(Bytes, Bytes)>>,
}

/// Parses Cache-Control header to extract max-age and stale-while-revalidate.
fn parse_cache_control(header_value: &str) -> (Option<u64>, Option<u64>) {
    let mut max_age = None;
    let mut swr = None;
    for directive in header_value.split(',') {
        let parts: Vec<&str> = directive.trim().splitn(2, '=').collect();
        if parts.len() == 2 {
            let key = parts[0];
            let value = parts[1];
            if key.eq_ignore_ascii_case("max-age") || key.eq_ignore_ascii_case("s-maxage") {
                if let Ok(seconds) = value.parse::<u64>() {
                    max_age = Some(seconds);
                }
            } else if key.eq_ignore_ascii_case("stale-while-revalidate")
                && let Ok(seconds) = value.parse::<u64>()
            {
                swr = Some(seconds);
            }
        }
    }
    (max_age, swr)
}

impl ParseCommand for CacheFetch {
    /// Parses the command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("CACHE.FETCH".to_string()));
        }

        let url_bytes = extract_bytes(&args[1])?;
        let url = String::from_utf8(url_bytes.to_vec())?;

        let mut cmd = CacheFetch {
            key: extract_bytes(&args[0])?,
            url,
            ..Default::default()
        };

        let mut parser = ArgParser::new(&args[2..]);
        let mut tags_found = false;
        let mut headers_found = false;

        while !parser.remaining_args().is_empty() {
            if tags_found || headers_found {
                break;
            }

            if let Some(seconds) = parser.match_option("ttl")? {
                cmd.ttl = Some(seconds);
            } else if let Some(swr_val) = parser.match_option("swr")? {
                cmd.swr = Some(swr_val);
            } else if let Some(grace_val) = parser.match_option("grace")? {
                cmd.grace = Some(grace_val);
            } else if let Some(vary_val) = parser.match_option::<String>("vary")? {
                cmd.vary = Some(Bytes::from(vary_val));
            } else if parser.match_flag("headers") {
                headers_found = true;
                break;
            } else if parser.match_flag("tags") {
                tags_found = true;
                break;
            } else {
                return Err(SpinelDBError::SyntaxError);
            }
        }

        let remaining = parser.remaining_args();
        if headers_found {
            if !remaining.len().is_multiple_of(2) {
                return Err(SpinelDBError::WrongArgumentCount(
                    "CACHE.FETCH HEADERS".to_string(),
                ));
            }
            cmd.headers = Some(
                remaining
                    .chunks_exact(2)
                    .map(|c| (extract_bytes(&c[0]).unwrap(), extract_bytes(&c[1]).unwrap()))
                    .collect(),
            );
        } else if tags_found {
            cmd.tags = remaining
                .iter()
                .map(extract_bytes)
                .collect::<Result<_, _>>()?;
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for CacheFetch {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let route_response = self.execute_and_stream(ctx).await?;

        // A successful fetch implies a write operation occurred.
        let write_outcome = match route_response {
            RouteResponse::NoOp => WriteOutcome::DidNotWrite,
            _ => WriteOutcome::Write { keys_modified: 1 },
        };

        let resp_value = match route_response {
            RouteResponse::Single(val) => val,
            RouteResponse::StreamBody { mut file, .. } => {
                let mut body = Vec::new();
                file.read_to_end(&mut body).await?;
                RespValue::BulkString(body.into())
            }
            RouteResponse::NoOp => RespValue::Null,
            _ => {
                return Err(SpinelDBError::Internal(
                    "Unexpected response type from stream logic".into(),
                ));
            }
        };

        Ok((resp_value, write_outcome))
    }
}

impl CacheFetch {
    /// Core execution logic for `CACHE.FETCH` that supports streaming responses.
    pub async fn execute_and_stream<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<RouteResponse, SpinelDBError> {
        let (allowed_domains, allow_private) = {
            let config = ctx.state.config.lock().await;
            (
                config.security.allowed_fetch_domains.clone(),
                config.security.allow_private_fetch_ips,
            )
        };
        validate_fetch_url(&self.url, &allowed_domains, allow_private).await?;

        // Bypass cache store and shared future logic for authorized requests.
        if self
            .headers
            .iter()
            .flatten()
            .any(|(k, _)| k.eq_ignore_ascii_case(b"authorization"))
        {
            debug!(
                "Bypassing cache store for authorized request to '{}'",
                self.url
            );
            let (outcome, _) = self.fetch_from_origin(&ctx.state, true).await?;
            let body_bytes = match outcome {
                FetchOutcome::InMemory(bytes) => bytes,
                FetchOutcome::OnDisk { path, .. } => tokio::fs::read(&path).await?.into(),
                FetchOutcome::Negative { status, body } => {
                    return Err(SpinelDBError::InvalidState(format!(
                        "Origin responded with status {status}: {}",
                        String::from_utf8_lossy(&body.unwrap_or_default())
                    )));
                }
            };
            return Ok(RouteResponse::Single(RespValue::BulkString(body_bytes)));
        }

        // Attempt an initial non-blocking read from the cache.
        let get_cmd = CacheGet {
            key: self.key.clone(),
            revalidate_url: None,
            headers: self.headers.clone(),
            ..Default::default()
        };
        let initial_response = get_cmd.execute_and_stream(ctx).await?;
        if !matches!(initial_response, RouteResponse::NoOp) {
            return Ok(initial_response);
        }

        // --- Cache Stampede Protection using a Shared Future ---
        let state = ctx.state.clone();
        let key = self.key.clone();

        let future_to_await = match state.cache.fetch_locks.entry(key.clone()) {
            // Follower path: an origin fetch is already in progress.
            dashmap::mapref::entry::Entry::Occupied(occupied) => {
                debug!(
                    "Cache miss for '{}': Another client is fetching. Awaiting shared result.",
                    String::from_utf8_lossy(&key)
                );
                occupied.get().clone()
            }
            // Leader path: this is the first client to request the missed key.
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                debug!(
                    "Cache miss for '{}'. This client is the leader and will fetch from origin.",
                    String::from_utf8_lossy(&key)
                );

                let state_clone = state.clone();
                let command_clone = self.clone();

                let fetch_future: BoxFuture<'static, Result<FetchOutcome, Arc<SpinelDBError>>> =
                    async move {
                        match command_clone.fetch_from_origin(&state_clone, false).await {
                            Ok((outcome, write_outcome)) => {
                                // The leader is responsible for updating the dirty keys counter.
                                if let WriteOutcome::Write { keys_modified } = write_outcome {
                                    state_clone.persistence.increment_dirty_keys(keys_modified);
                                }
                                Ok(outcome)
                            }
                            Err(e) => Err(Arc::new(e)),
                        }
                    }
                    .boxed();

                let shared_future = fetch_future.shared();
                vacant.insert(shared_future.clone());
                shared_future
            }
        };

        // All clients (leader and followers) await the shared result here.
        let fetch_result = future_to_await.await;

        // The operation is complete; remove the future from the map to prevent memory leaks.
        state.cache.fetch_locks.remove(&key);

        match fetch_result {
            Ok(outcome) => {
                // Each client constructs its own response from the shared outcome.
                match outcome {
                    FetchOutcome::InMemory(bytes) => {
                        Ok(RouteResponse::Single(RespValue::BulkString(bytes)))
                    }
                    FetchOutcome::OnDisk { path, size } => {
                        let file = TokioFile::open(&path).await.map_err(|e| {
                            SpinelDBError::Internal(format!(
                                "Failed to open cache file for streaming: {e}"
                            ))
                        })?;
                        let resp_header = format!("${size}\r\n").into_bytes();
                        Ok(RouteResponse::StreamBody { resp_header, file })
                    }
                    FetchOutcome::Negative { status, body } => {
                        Err(SpinelDBError::InvalidState(format!(
                            "Origin responded with status {status}: {}",
                            String::from_utf8_lossy(&body.unwrap_or_default())
                        )))
                    }
                }
            }
            Err(arc_err) => Err(SpinelDBError::clone(&*arc_err)),
        }
    }

    /// Fetches from the origin, deciding whether to stream to disk or buffer in memory.
    pub async fn fetch_from_origin(
        &self,
        server_state: &Arc<ServerState>,
        mut bypass_store: bool,
    ) -> Result<(FetchOutcome, WriteOutcome), SpinelDBError> {
        let (streaming_threshold, cache_path, global_negative_ttl) = {
            let config = server_state.config.lock().await;
            (
                config.cache.streaming_threshold_bytes,
                config.cache.on_disk_path.clone(),
                config.cache.negative_cache_ttl_seconds,
            )
        };

        let key_str = String::from_utf8_lossy(&self.key);
        let policies = server_state.cache.policies.read().await;
        let matched_policy = policies
            .iter()
            .find(|p| WildMatch::new(&p.key_pattern).matches(&key_str));

        let respect_origin = matched_policy.is_some_and(|p| p.respect_origin_headers);
        let policy_negative_ttl = matched_policy.and_then(|p| p.negative_ttl);

        server_state.cache.increment_misses();
        let client = reqwest::Client::new();
        let mut res = client
            .get(&self.url)
            .send()
            .await
            .map_err(|e| SpinelDBError::HttpClientError(e.to_string()))?;

        if res.status() != reqwest::StatusCode::OK {
            let status = res.status();
            let error_body = res.bytes().await.ok();

            let negative_ttl = policy_negative_ttl.unwrap_or(global_negative_ttl);
            if !bypass_store && negative_ttl > 0 {
                let db = server_state.get_db(0).unwrap();
                let set_cmd_for_lock = Command::Cache(crate::core::commands::cache::Cache {
                    subcommand: crate::core::commands::cache::command::CacheSubcommand::Set(
                        CacheSet::default(),
                    ),
                });
                let mut temp_ctx = ExecutionContext {
                    state: server_state.clone(),
                    locks: db.determine_locks_for_command(&set_cmd_for_lock).await,
                    db: &db,
                    command: Some(set_cmd_for_lock),
                    session_id: 0,
                    authenticated_user: None,
                };
                let set_cmd_internal = CacheSet {
                    key: self.key.clone(),
                    ttl: Some(negative_ttl),
                    tags: self.tags.clone(),
                    vary: self.vary.clone(),
                    headers: self.headers.clone(),
                    ..Default::default()
                };
                let _ = set_cmd_internal
                    .execute_internal(
                        &mut temp_ctx,
                        CacheBody::Negative {
                            status: status.as_u16(),
                            body: error_body.clone(),
                        },
                    )
                    .await;
                warn!(
                    "Stored negative cache entry for key '{}' due to origin status {}",
                    String::from_utf8_lossy(&self.key),
                    status
                );
            }
            return Ok((
                FetchOutcome::Negative {
                    status: status.as_u16(),
                    body: error_body,
                },
                WriteOutcome::DidNotWrite,
            ));
        }

        let headers = res.headers().clone();

        let (mut ttl_override, mut swr_override) = (self.ttl, self.swr);
        if respect_origin
            && let Some(cc_header) = headers
                .get(reqwest::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
        {
            let (parsed_ttl, parsed_swr) = parse_cache_control(cc_header);
            if parsed_ttl.is_some() {
                ttl_override = parsed_ttl;
            }
            if parsed_swr.is_some() {
                swr_override = parsed_swr;
            }
        }

        if headers
            .get(reqwest::header::VARY)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.split(',').any(|part| part.trim() == "*"))
        {
            debug!("Origin responded with 'Vary: *'. Bypassing cache store.");
            bypass_store = true;
        }

        let content_length = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());

        let should_stream_to_disk = !bypass_store
            && (content_length.is_none() || content_length.unwrap_or(0) >= streaming_threshold);

        let final_cache_body;
        let final_outcome_for_client;

        if should_stream_to_disk {
            tokio::fs::create_dir_all(&cache_path).await?;
            let final_filename = Uuid::new_v4().to_string();
            let final_path = PathBuf::from(&cache_path).join(&final_filename);

            server_state
                .cache
                .log_manifest(self.key.clone(), ManifestState::Pending, final_path.clone())
                .await?;

            let mut temp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&final_path)
                .await?;
            let mut total_size = 0;
            while let Some(chunk) = res.chunk().await? {
                temp_file.write_all(&chunk).await?;
                total_size += chunk.len();
            }
            temp_file.sync_all().await?;
            drop(temp_file);

            if total_size < streaming_threshold {
                let body_bytes = tokio::fs::read(&final_path).await?;
                tokio::fs::remove_file(&final_path).await.ok();
                final_cache_body = CacheBody::InMemory(body_bytes.clone().into());
                final_outcome_for_client = FetchOutcome::InMemory(body_bytes.into());
            } else {
                final_cache_body = CacheBody::OnDisk {
                    path: final_path.clone(),
                    size: total_size as u64,
                };
                final_outcome_for_client = FetchOutcome::OnDisk {
                    path: final_path,
                    size: total_size as u64,
                };
            }
        } else {
            let body_bytes = res.bytes().await?;
            final_cache_body = CacheBody::InMemory(body_bytes.clone());
            final_outcome_for_client = FetchOutcome::InMemory(body_bytes);
        }

        if bypass_store {
            return Ok((final_outcome_for_client, WriteOutcome::DidNotWrite));
        }

        let set_cmd_internal = CacheSet {
            key: self.key.clone(),
            body_data: Bytes::new(),
            ttl: ttl_override,
            swr: swr_override,
            grace: self.grace,
            revalidate_url: Some(self.url.clone()),
            etag: headers
                .get(reqwest::header::ETAG)
                .map(|v| Bytes::from(v.as_bytes().to_vec())),
            last_modified: headers
                .get(reqwest::header::LAST_MODIFIED)
                .map(|v| Bytes::from(v.as_bytes().to_vec())),
            tags: self.tags.clone(),
            vary: self.vary.clone(),
            headers: self.headers.clone(),
            ..Default::default()
        };

        let db = server_state.get_db(0).unwrap();
        let set_cmd_for_lock = Command::Cache(crate::core::commands::cache::Cache {
            subcommand: crate::core::commands::cache::command::CacheSubcommand::Set(
                set_cmd_internal.clone(),
            ),
        });
        let mut set_ctx = ExecutionContext {
            state: server_state.clone(),
            locks: db.determine_locks_for_command(&set_cmd_for_lock).await,
            db: &db,
            command: Some(set_cmd_for_lock),
            session_id: 0,
            authenticated_user: None,
        };

        let (_, write_outcome) = set_cmd_internal
            .execute_internal(&mut set_ctx, final_cache_body.clone())
            .await?;

        if let CacheBody::OnDisk { path, .. } = final_cache_body {
            server_state
                .cache
                .log_manifest(set_cmd_internal.key.clone(), ManifestState::Committed, path)
                .await?;
        }
        Ok((final_outcome_for_client, write_outcome))
    }
}

impl CommandSpec for CacheFetch {
    fn name(&self) -> &'static str {
        "cache.fetch"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
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
        let mut args = vec![self.key.clone(), self.url.clone().into()];
        if let Some(ttl) = self.ttl {
            args.extend([Bytes::from_static(b"TTL"), ttl.to_string().into()]);
        }
        if let Some(swr) = self.swr {
            args.extend([Bytes::from_static(b"SWR"), swr.to_string().into()]);
        }
        if let Some(grace) = self.grace {
            args.extend([Bytes::from_static(b"GRACE"), grace.to_string().into()]);
        }
        if let Some(v) = &self.vary {
            args.extend([Bytes::from_static(b"VARY"), v.clone()]);
        }
        if !self.tags.is_empty() {
            args.push(Bytes::from_static(b"TAGS"));
            args.extend(self.tags.clone());
        }
        if let Some(h) = &self.headers {
            args.push(Bytes::from_static(b"HEADERS"));
            args.extend(h.iter().flat_map(|(k, v)| vec![k.clone(), v.clone()]));
        }
        args
    }
}
