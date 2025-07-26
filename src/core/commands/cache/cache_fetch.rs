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
use crate::core::storage::cache_types::{CacheBody, ManifestState};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::{debug, warn};
use uuid::Uuid;

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
            if remaining.len() % 2 != 0 {
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
    /// Executes the `CACHE.FETCH` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (allowed_domains, allow_private) = {
            let config = ctx.state.config.lock().await;
            (
                config.security.allowed_fetch_domains.clone(),
                config.security.allow_private_fetch_ips,
            )
        };
        validate_fetch_url(&self.url, &allowed_domains, allow_private).await?;

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
            let (body, _) = self.fetch_from_origin(ctx, true).await?;
            return Ok((RespValue::BulkString(body), WriteOutcome::DidNotWrite));
        }

        let get_cmd = CacheGet {
            key: self.key.clone(),
            revalidate_url: None,
            headers: self.headers.clone(),
            ..Default::default()
        };

        let initial_response = get_cmd.execute_and_stream(ctx).await?;
        if !matches!(initial_response, RouteResponse::NoOp) {
            return match initial_response {
                RouteResponse::Single(val) => Ok((val, WriteOutcome::DidNotWrite)),
                RouteResponse::StreamBody { mut file, .. } => {
                    let mut body = Vec::new();
                    file.read_to_end(&mut body).await?;
                    Ok((
                        RespValue::BulkString(body.into()),
                        WriteOutcome::DidNotWrite,
                    ))
                }
                _ => unreachable!(),
            };
        }

        let fetch_lock = ctx
            .state
            .cache
            .fetch_locks
            .entry(self.key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _lock_guard = fetch_lock.lock().await;

        let double_check_response = get_cmd.execute_and_stream(ctx).await?;
        if !matches!(double_check_response, RouteResponse::NoOp) {
            ctx.state.cache.fetch_locks.remove(&self.key);
            return match double_check_response {
                RouteResponse::Single(val) => Ok((val, WriteOutcome::DidNotWrite)),
                RouteResponse::StreamBody { mut file, .. } => {
                    let mut body = Vec::new();
                    file.read_to_end(&mut body).await?;
                    Ok((
                        RespValue::BulkString(body.into()),
                        WriteOutcome::DidNotWrite,
                    ))
                }
                _ => unreachable!(),
            };
        }

        debug!(
            "Cache miss for key '{}'. This client is the leader and will fetch from origin.",
            String::from_utf8_lossy(&self.key)
        );

        let fetch_result = self.fetch_from_origin(ctx, false).await;
        ctx.state.cache.fetch_locks.remove(&self.key);

        match fetch_result {
            Ok((body, write_outcome)) => Ok((RespValue::BulkString(body), write_outcome)),
            Err(e) => Err(e),
        }
    }
}

impl CacheFetch {
    /// Fetches from the origin, deciding whether to stream to disk or buffer in memory.
    pub async fn fetch_from_origin(
        &self,
        ctx: &mut ExecutionContext<'_>,
        mut bypass_store: bool,
    ) -> Result<(Bytes, WriteOutcome), SpinelDBError> {
        let (streaming_threshold, cache_path, negative_cache_ttl) = {
            let config = ctx.state.config.lock().await;
            (
                config.cache.streaming_threshold_bytes,
                config.cache.on_disk_path.clone(),
                config.cache.negative_cache_ttl_seconds,
            )
        };

        ctx.state.cache.increment_misses();
        let client = reqwest::Client::new();
        let mut res = client
            .get(&self.url)
            .send()
            .await
            .map_err(|e| SpinelDBError::HttpClientError(e.to_string()))?;

        if res.status() != reqwest::StatusCode::OK {
            let status = res.status();
            let error_message = format!("Origin server responded with status {status}");

            if !bypass_store && negative_cache_ttl > 0 {
                let set_cmd_internal = CacheSet {
                    key: self.key.clone(),
                    body_data: Bytes::from(status.as_u16().to_string()),
                    ttl: Some(negative_cache_ttl),
                    swr: Some(0),
                    grace: Some(0),
                    tags: self.tags.clone(),
                    vary: self.vary.clone(),
                    headers: self.headers.clone(),
                    etag: Some(Bytes::from_static(b"__NEGATIVE_CACHE__")),
                    ..Default::default()
                };

                let _ = set_cmd_internal
                    .execute_internal(ctx, CacheBody::InMemory(set_cmd_internal.body_data.clone()))
                    .await;

                warn!(
                    "Stored negative cache entry for key '{}' due to origin status {}",
                    String::from_utf8_lossy(&self.key),
                    status
                );
            }

            return Err(SpinelDBError::Internal(error_message));
        }

        let headers = res.headers().clone();

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
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let final_cache_body;
        let final_body_for_client;

        if !bypass_store && content_length >= streaming_threshold {
            tokio::fs::create_dir_all(&cache_path).await?;
            let final_filename = Uuid::new_v4().to_string();
            let final_path = PathBuf::from(&cache_path).join(&final_filename);

            ctx.state
                .cache
                .log_manifest(ManifestState::Pending, final_path.clone())
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

            final_cache_body = CacheBody::OnDisk {
                path: final_path.clone(),
                size: total_size as u64,
            };
            final_body_for_client = tokio::fs::read(&final_path).await?.into();
        } else {
            let body_bytes = res.bytes().await?;
            final_cache_body = CacheBody::InMemory(body_bytes.clone());
            final_body_for_client = body_bytes;
        }

        if bypass_store {
            return Ok((final_body_for_client, WriteOutcome::DidNotWrite));
        }

        let set_cmd_internal = CacheSet {
            key: self.key.clone(),
            body_data: Bytes::new(),
            ttl: self.ttl,
            swr: self.swr,
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
        };

        let (_, write_outcome) = set_cmd_internal
            .execute_internal(ctx, final_cache_body.clone())
            .await?;

        if let CacheBody::OnDisk { path, .. } = final_cache_body {
            ctx.state
                .cache
                .log_manifest(ManifestState::Committed, path)
                .await?;
        }

        Ok((final_body_for_client, write_outcome))
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
