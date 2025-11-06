// src/core/commands/cache/cache_set.rs
//! Implements the `CACHE.SET` command, which stores an object in the cache with
//! advanced options for TTL, tagging, and content negotiation.

use super::helpers::calculate_variant_hash;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_bytes, validate_fetch_url};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::cache_types::{CacheBody, CacheVariant, HttpMetadata};
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tracing::debug;
use uuid::Uuid;
use wildmatch::WildMatch;

/// Represents the `CACHE.SET` command with all its parsed options.
#[derive(Debug, Clone, Default)]
pub struct CacheSet {
    pub key: Bytes,
    /// The body of the object to cache, parsed from the command arguments.
    pub body_data: Bytes,
    pub ttl: Option<u64>,
    pub etag: Option<Bytes>,
    pub last_modified: Option<Bytes>,
    pub tags: Vec<Bytes>,
    pub vary: Option<Bytes>,
    pub headers: Option<Vec<(Bytes, Bytes)>>,
    pub swr: Option<u64>,
    pub grace: Option<u64>,
    pub revalidate_url: Option<String>,
    /// If true, the body will be compressed with zstd before storing.
    pub compression: bool,
    /// If true, the body will be stored on disk, regardless of its size.
    pub force_disk: bool,
}

impl ParseCommand for CacheSet {
    /// Parses the `CACHE.SET` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("CACHE.SET".to_string()));
        }

        let mut cmd = CacheSet {
            key: extract_bytes(&args[0])?,
            body_data: extract_bytes(&args[1])?,
            ..Default::default()
        };

        let mut parser = ArgParser::new(&args[2..]);
        let mut tags_found = false;
        let mut headers_found = false;

        // Iteratively parse optional arguments.
        while !parser.remaining_args().is_empty() {
            if tags_found || headers_found {
                break;
            }

            if let Some(seconds) = parser.match_option("ttl")? {
                cmd.ttl = Some(seconds);
            } else if let Some(etag_val) = parser.match_option::<String>("etag")? {
                cmd.etag = Some(Bytes::from(etag_val));
            } else if let Some(lm_val) = parser.match_option::<String>("last-modified")? {
                cmd.last_modified = Some(Bytes::from(lm_val));
            } else if let Some(vary_val) = parser.match_option::<String>("vary")? {
                cmd.vary = Some(Bytes::from(vary_val));
            } else if let Some(swr_val) = parser.match_option("swr")? {
                cmd.swr = Some(swr_val);
            } else if let Some(grace_val) = parser.match_option("grace")? {
                cmd.grace = Some(grace_val);
            } else if let Some(url) = parser.match_option("revalidate-url")? {
                cmd.revalidate_url = Some(url);
            } else if parser.match_flag("compression") {
                cmd.compression = true;
            } else if parser.match_flag("force-disk") {
                cmd.force_disk = true;
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
                    "CACHE.SET HEADERS".to_string(),
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

/// Sets the expiry, SWR, and Grace timestamps on a StoredValue based on command options.
pub(super) fn apply_ttl_options(
    value: &mut StoredValue,
    ttl: Option<u64>,
    swr: Option<u64>,
    grace: Option<u64>,
) {
    let now = Instant::now();
    if let Some(ttl_seconds) = ttl {
        if ttl_seconds > 0 {
            let fresh_duration = Duration::from_secs(ttl_seconds);
            value.expiry = Some(now + fresh_duration);

            let swr_duration = Duration::from_secs(swr.unwrap_or(0));
            value.stale_revalidate_expiry = Some(now + fresh_duration + swr_duration);

            let grace_duration = Duration::from_secs(grace.unwrap_or(0));
            value.grace_expiry = Some(now + fresh_duration + swr_duration + grace_duration);
        } else {
            // A TTL of 0 means the item has no expiry.
            value.expiry = None;
            value.stale_revalidate_expiry = None;
            value.grace_expiry = None;
        }
    }
}

#[async_trait]
impl ExecutableCommand for CacheSet {
    /// Executes the `CACHE.SET` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if let Some(url) = &self.revalidate_url {
            let config = ctx.state.config.lock().await;
            validate_fetch_url(
                url,
                &config.security.allowed_fetch_domains,
                config.security.allow_private_fetch_ips,
            )
            .await?;
        }

        // Determine the storage method for the cache body based on command flags.
        let cache_path = ctx.state.config.lock().await.cache.on_disk_path.clone();
        let cache_body = if self.force_disk {
            if cache_path.is_empty() {
                return Err(SpinelDBError::InvalidState(
                    "Cannot use FORCE-DISK, on_disk_path is not configured.".into(),
                ));
            }
            // Forcefully write the body to a file on disk.
            let final_filename = Uuid::new_v4().to_string();
            let final_path = PathBuf::from(&cache_path).join(&final_filename);
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&final_path)
                .await?;
            file.write_all(&self.body_data).await?;
            file.sync_all().await?;
            CacheBody::OnDisk {
                path: final_path,
                size: self.body_data.len() as u64,
            }
        } else if self.compression {
            // Compress the body in memory.
            let original_size = self.body_data.len();
            let compressed_data = Bytes::from(zstd::encode_all(self.body_data.as_ref(), 0)?);
            CacheBody::CompressedInMemory {
                original_size,
                data: compressed_data,
            }
        } else {
            // Default behavior: store the body in memory.
            CacheBody::InMemory(self.body_data.clone())
        };

        self.execute_internal(ctx, cache_body).await
    }
}

impl CacheSet {
    /// Internal execution logic that accepts a pre-constructed `CacheBody`.
    pub async fn execute_internal<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
        cache_body: CacheBody,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state_clone = ctx.state.clone();
        let max_variants = state_clone.config.lock().await.cache.max_variants_per_key;

        let needs_prewarm = {
            let policies = state_clone.cache.policies.read().await;
            if policies.is_empty() {
                false
            } else {
                let key_str = String::from_utf8_lossy(&self.key);
                policies
                    .iter()
                    .any(|p| p.prewarm && WildMatch::new(&p.key_pattern).matches(&key_str))
            }
        };

        let (_, guard) = ctx.get_single_shard_context_mut()?;

        let data_clone = if let Some(entry) = guard.peek(&self.key) {
            if !matches!(entry.data, DataValue::HttpCache { .. }) {
                return Err(SpinelDBError::WrongType);
            }
            entry.data.clone()
        } else {
            DataValue::HttpCache {
                variants: HashMap::new(),
                vary_on: vec![],
                tags_epoch: 0,
            }
        };

        let (mut variants, mut vary_on, mut tags_epoch) = match data_clone {
            DataValue::HttpCache {
                variants,
                vary_on,
                tags_epoch,
            } => (variants, vary_on, tags_epoch),
            _ => unreachable!(),
        };

        if let Some(v) = &self.vary {
            let vary_str = std::str::from_utf8(v)?;
            let new_vary_on: Vec<Bytes> = vary_str
                .split(',')
                .map(|s| Bytes::from(s.trim().to_string()))
                .collect();

            if vary_on != new_vary_on {
                variants.clear();
                vary_on = new_vary_on;
            }
        } else if !vary_on.is_empty() {
            variants.clear();
            vary_on.clear();
        }

        if let Some(cluster) = &state_clone.cluster {
            tags_epoch = cluster.last_purge_epoch.load(Ordering::Relaxed);
        }

        let content_encoding = if matches!(cache_body, CacheBody::CompressedInMemory { .. }) {
            Some(Bytes::from_static(b"zstd"))
        } else {
            None
        };

        let new_variant = CacheVariant {
            body: cache_body,
            metadata: HttpMetadata {
                etag: self.etag.clone(),
                last_modified: self.last_modified.clone(),
                revalidate_url: self.revalidate_url.clone(),
                content_encoding,
            },
            last_accessed: Instant::now(),
        };

        let variant_hash = calculate_variant_hash(&vary_on, &self.headers);

        if max_variants > 0
            && variants.len() >= max_variants
            && !variants.contains_key(&variant_hash)
            && let Some(lru_hash) = variants
                .iter()
                .min_by_key(|(_, v)| v.last_accessed)
                .map(|(h, _)| *h)
        {
            variants.remove(&lru_hash);
            debug!(
                "Evicted LRU variant for key '{}' to make space for new variant.",
                String::from_utf8_lossy(&self.key)
            );
        }

        variants.insert(variant_hash, new_variant);

        let mut new_stored_value = StoredValue::new(DataValue::HttpCache {
            variants,
            vary_on,
            tags_epoch,
        });

        apply_ttl_options(&mut new_stored_value, self.ttl, self.swr, self.grace);

        guard.remove_key_from_tags(&self.key);
        guard.add_tags_for_key(self.key.clone(), &self.tags);
        guard.put(self.key.clone(), new_stored_value);

        if needs_prewarm {
            state_clone
                .cache
                .prewarm_keys
                .write()
                .await
                .insert(self.key.clone());
        }

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for CacheSet {
    fn name(&self) -> &'static str {
        "cache.set"
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
        let mut args = vec![self.key.clone(), self.body_data.clone()];
        if let Some(ttl) = self.ttl {
            args.extend([Bytes::from_static(b"TTL"), ttl.to_string().into()]);
        }
        if let Some(swr) = self.swr {
            args.extend([Bytes::from_static(b"SWR"), swr.to_string().into()]);
        }
        if let Some(grace) = self.grace {
            args.extend([Bytes::from_static(b"GRACE"), grace.to_string().into()]);
        }
        if let Some(url) = &self.revalidate_url {
            args.extend([Bytes::from_static(b"REVALIDATE-URL"), url.clone().into()]);
        }
        if let Some(etag) = &self.etag {
            args.extend([Bytes::from_static(b"ETAG"), etag.clone()]);
        }
        if let Some(lm) = &self.last_modified {
            args.extend([Bytes::from_static(b"LAST-MODIFIED"), lm.clone()]);
        }
        if let Some(v) = &self.vary {
            args.extend([Bytes::from_static(b"VARY"), v.clone()]);
        }
        if self.compression {
            args.push(Bytes::from_static(b"COMPRESSION"));
        }
        if self.force_disk {
            args.push(Bytes::from_static(b"FORCE-DISK"));
        }
        if let Some(h) = &self.headers {
            args.push(Bytes::from_static(b"HEADERS"));
            args.extend(h.iter().flat_map(|(k, v)| vec![k.clone(), v.clone()]));
        }
        if !self.tags.is_empty() {
            args.push(Bytes::from_static(b"TAGS"));
            args.extend(self.tags.clone());
        }
        args
    }
}
