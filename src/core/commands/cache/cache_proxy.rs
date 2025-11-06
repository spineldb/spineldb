// src/core/commands/cache/cache_proxy.rs
//! Implements the `CACHE.PROXY` command, which provides a convenient
//! get-or-fetch pattern. It attempts to retrieve a key, and if it's a
//! cache miss, it automatically fetches from an origin and caches the result.

use crate::core::commands::cache::cache_fetch::{CacheFetch, FetchOutcome};
use crate::core::commands::cache::cache_get::CacheGet;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::handler::command_router::RouteResponse;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use regex::Regex;
use tokio::io::AsyncReadExt;
use tracing::debug;
use urlencoding::encode;
use wildmatch::WildMatch;

/// Converts a simple glob pattern (`*`) to a regex pattern for URL interpolation.
fn glob_to_regex(glob: &str) -> String {
    let mut regex = String::with_capacity(glob.len() * 2);
    regex.push('^');
    for c in glob.chars() {
        match c {
            '*' => regex.push_str("(.*)"), // Capture group for interpolation
            '?' => regex.push('.'),
            c if ".+()|\\{}[]^$".contains(c) => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

/// Implements the `CACHE.PROXY` command.
#[derive(Debug, Clone, Default)]
pub struct CacheProxy {
    pub key: Bytes,
    pub url: Option<String>,
    pub ttl: Option<u64>,
    pub swr: Option<u64>,
    pub grace: Option<u64>,
    pub tags: Vec<Bytes>,
    pub vary: Option<Bytes>,
    pub headers: Option<Vec<(Bytes, Bytes)>>,
}

impl ParseCommand for CacheProxy {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CACHE.PROXY".to_string()));
        }

        let mut cmd = CacheProxy {
            key: extract_bytes(&args[0])?,
            ..Default::default()
        };

        let mut i = 1;
        // Check if the second argument is a URL or an option.
        if let Some(arg) = args.get(i)
            && let Ok(s) = extract_string(arg)
        {
            let s_lower = s.to_ascii_lowercase();
            if s_lower != "ttl"
                && s_lower != "swr"
                && s_lower != "grace"
                && s_lower != "tags"
                && s_lower != "headers"
                && s_lower != "vary"
            {
                cmd.url = Some(s);
                i += 1;
            }
        }

        // If no explicit URL is provided, check if the key itself is a URL.
        // This allows for a simpler command: `CACHE.PROXY <url> [options...]`
        if cmd.url.is_none()
            && let Ok(key_str) = std::str::from_utf8(&cmd.key)
            && (key_str.starts_with("http://") || key_str.starts_with("https://"))
        {
            cmd.url = Some(key_str.to_string());
        }
        let mut parser = ArgParser::new(&args[i..]);
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
                    "CACHE.PROXY HEADERS".to_string(),
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
impl ExecutableCommand for CacheProxy {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match self.execute_and_stream(ctx).await? {
            RouteResponse::Single(val) => Ok((val, WriteOutcome::DidNotWrite)),
            RouteResponse::NoOp => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            RouteResponse::StreamBody { mut file, .. } => {
                let mut body = Vec::new();
                file.read_to_end(&mut body).await?;
                Ok((
                    RespValue::BulkString(body.into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            _ => Err(SpinelDBError::Internal(
                "Unexpected response from proxy stream logic".into(),
            )),
        }
    }
}

impl CacheProxy {
    /// Core execution logic for `CACHE.PROXY` that supports streaming responses.
    pub async fn execute_and_stream<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<RouteResponse, SpinelDBError> {
        // Step 1: Attempt a non-blocking read using CACHE.GET logic.
        let get_cmd = CacheGet {
            key: self.key.clone(),
            headers: self.headers.clone(),
            ..Default::default()
        };

        let get_response = get_cmd.execute_and_stream(ctx).await?;

        // If we got a hit, return it directly.
        if !matches!(get_response, RouteResponse::NoOp) {
            return Ok(get_response);
        }

        // On cache miss, release the lock before starting the potentially long fetch operation.
        ctx.release_locks();

        debug!(
            "CACHE.PROXY miss for key '{}'. Proceeding to fetch.",
            String::from_utf8_lossy(&self.key)
        );

        // Step 2: On miss, resolve policy and construct a CACHE.FETCH command.
        let mut resolved_url = self.url.clone();
        let mut resolved_ttl = self.ttl;
        let mut resolved_swr = self.swr;
        let mut resolved_grace = self.grace;
        let mut resolved_tags = self.tags.clone();
        let mut resolved_vary_on: Option<Bytes> = self.vary.clone();
        let mut relevant_headers = self.headers.clone();
        let mut policy_name = "none";

        let key_str = String::from_utf8_lossy(&self.key);
        let policies = ctx.state.cache.policies.read().await.clone();

        // Find the highest-priority matching policy. Policies are pre-sorted on SET.
        let matched_policy = policies
            .iter()
            .find(|p| WildMatch::new(&p.key_pattern).matches(&key_str));

        if let Some(policy) = matched_policy {
            policy_name = &policy.name;
            debug!(
                "Matched cache policy '{}' for key '{}'",
                policy_name, key_str
            );

            // Resolve URL template if not provided in the command.
            if resolved_url.is_none() {
                let mut url = policy.url_template.clone();

                // Interpolate from request headers.
                let re_hdr = Regex::new(r"\{hdr:([^}]+)\}").unwrap();
                url = re_hdr
                    .replace_all(&url, |caps: &regex::Captures| {
                        let header_name = &caps[1];
                        self.headers
                            .as_ref()
                            .and_then(|h| {
                                h.iter()
                                    .find(|(k, _)| k.eq_ignore_ascii_case(header_name.as_bytes()))
                            })
                            .map(|(_, v)| encode(&String::from_utf8_lossy(v)).into_owned())
                            .unwrap_or_default()
                    })
                    .to_string();

                // Interpolate from key pattern captures.
                let re_key = Regex::new(&glob_to_regex(&policy.key_pattern)).unwrap();
                if let Some(caps) = re_key.captures(&key_str) {
                    for i in 1..caps.len() {
                        if let Some(capture) = caps.get(i) {
                            let placeholder = format!("{{{i}}}");
                            let sanitized_capture = encode(capture.as_str());
                            url = url.replace(&placeholder, &sanitized_capture);
                        }
                    }
                }
                resolved_url = Some(url);
            }

            // Inherit options from the policy if not specified in the command.
            resolved_ttl = self.ttl.or(policy.ttl);
            resolved_swr = self.swr.or(policy.swr);
            resolved_grace = self.grace.or(policy.grace);

            // If policy defines `vary_on`, use it to filter client headers.
            if !policy.vary_on.is_empty() {
                resolved_vary_on = Some(Bytes::from(policy.vary_on.join(",")));

                if let Some(all_client_headers) = &self.headers {
                    relevant_headers = Some(
                        all_client_headers
                            .iter()
                            .filter(|(name, _)| {
                                policy
                                    .vary_on
                                    .iter()
                                    .any(|h| h.eq_ignore_ascii_case(&String::from_utf8_lossy(name)))
                            })
                            .cloned()
                            .collect(),
                    );
                } else {
                    relevant_headers = None;
                }
            }

            // Generate dynamic tags from the policy's templates.
            let re_key = Regex::new(&glob_to_regex(&policy.key_pattern)).unwrap();
            if let Some(caps) = re_key.captures(&key_str) {
                for tag_template in &policy.tags {
                    let mut final_tag = tag_template.clone();
                    for i in 1..caps.len() {
                        if let Some(capture) = caps.get(i) {
                            let placeholder = format!("{{{i}}}");
                            final_tag = final_tag.replace(&placeholder, capture.as_str());
                        }
                    }
                    resolved_tags.push(Bytes::from(final_tag));
                }
            }
        }

        let final_url = resolved_url.ok_or_else(|| {
            SpinelDBError::InvalidState("No matching cache policy found and no URL provided".into())
        })?;

        // Step 3: Delegate the fetch-and-set logic to CACHE.FETCH.
        let fetch_cmd = CacheFetch {
            key: self.key.clone(),
            url: final_url,
            ttl: resolved_ttl,
            swr: resolved_swr,
            grace: resolved_grace,
            tags: resolved_tags,
            vary: resolved_vary_on,
            headers: relevant_headers,
        };

        // Update metrics with the resolved policy label.
        crate::core::metrics::CACHE_MISSES_TOTAL
            .with_label_values(&[policy_name])
            .inc();

        let (outcome, _write_outcome) = fetch_cmd.fetch_from_origin(&ctx.state, false).await?;

        match outcome {
            FetchOutcome::InMemory(bytes) => Ok(RouteResponse::Single(RespValue::Array(vec![
                RespValue::Integer(200),
                RespValue::Array(vec![]),
                RespValue::BulkString(bytes),
            ]))),
            FetchOutcome::OnDisk { path, .. } => {
                let body = tokio::fs::read(&path).await?.into();
                Ok(RouteResponse::Single(RespValue::Array(vec![
                    RespValue::Integer(200),
                    RespValue::Array(vec![]),
                    RespValue::BulkString(body),
                ])))
            }
            FetchOutcome::Negative { status, body } => {
                Ok(RouteResponse::Single(RespValue::Array(vec![
                    RespValue::Integer(status as i64),
                    RespValue::Array(vec![]),
                    RespValue::BulkString(body.unwrap_or_default()),
                ])))
            }
        }
    }
}

impl CommandSpec for CacheProxy {
    fn name(&self) -> &'static str {
        "cache.proxy"
    }

    fn arity(&self) -> i64 {
        -2
    }

    fn flags(&self) -> CommandFlags {
        // This command can result in a write, so it is marked as such.
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
        // This command is not propagated directly. The underlying CACHE.SET
        // generated by the fetch is propagated instead, ensuring replication is deterministic.
        vec![]
    }
}
