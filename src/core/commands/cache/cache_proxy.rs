// src/core/commands/cache/cache_proxy.rs

use crate::core::commands::cache::cache_fetch::CacheFetch;
use crate::core::commands::cache::cache_get::CacheGet;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_bytes, extract_string};
use crate::core::handler::command_router::RouteResponse;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use regex::Regex;
use tokio::io::AsyncReadExt;
use tracing::debug;
use urlencoding::encode;
use wildmatch::WildMatch;

/// Implements the `CACHE.PROXY` command, which provides a convenient
/// get-or-fetch pattern. It attempts to retrieve a key, and if it's a
/// cache miss, it automatically fetches from an origin and caches the result.
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

/// A helper function to convert a simple glob pattern (`*`) to a regex pattern.
fn glob_to_regex(glob: &str) -> String {
    let mut regex = String::with_capacity(glob.len() * 2);
    regex.push('^');
    for c in glob.chars() {
        match c {
            '*' => regex.push_str("(.*)"), // Capture group for interpolation
            '?' => regex.push('.'),
            '.' | '+' | '(' | ')' | '|' | '\\' | '{' | '}' | '[' | ']' | '^' | '$' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
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
        if let Some(arg) = args.get(i) {
            if let Ok(s) = extract_string(arg) {
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
            if remaining.len() % 2 != 0 {
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
        // This is a fallback for the standard execution path.
        // The primary logic is in `execute_and_stream`.
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

        // If we got a hit (either in-memory or a file stream), return it directly.
        if !matches!(get_response, RouteResponse::NoOp) {
            return Ok(get_response);
        }

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

        if resolved_url.is_none() {
            let key_str = String::from_utf8_lossy(&self.key);
            let policies = ctx.state.cache.policies.read().await;

            let matched_policy = policies
                .iter()
                .find(|p| WildMatch::new(&p.key_pattern).matches(&key_str))
                .cloned();

            if let Some(policy) = matched_policy {
                debug!(
                    "Matched cache policy '{}' for key '{}'",
                    policy.name, key_str
                );
                let mut url = policy.url_template.clone();
                let regex_pattern = glob_to_regex(&policy.key_pattern);
                if let Ok(re) = Regex::new(&regex_pattern) {
                    if let Some(caps) = re.captures(&key_str) {
                        for i in 1..caps.len() {
                            if let Some(capture) = caps.get(i) {
                                let placeholder = format!("{{{i}}}");
                                // Sanitize the captured value to prevent path traversal
                                let sanitized_capture = encode(capture.as_str());
                                url = url.replace(&placeholder, &sanitized_capture);
                            }
                        }
                    }
                }
                resolved_url = Some(url);
                resolved_ttl = self.ttl.or(policy.ttl);
                resolved_swr = self.swr.or(policy.swr);
                resolved_grace = self.grace.or(policy.grace);
                let policy_tags: Vec<Bytes> = policy.tags.into_iter().map(Bytes::from).collect();
                resolved_tags.extend(policy_tags);
            } else {
                return Err(SpinelDBError::InvalidState(
                    "No matching cache policy found and no URL provided".into(),
                ));
            }
        }

        // Step 3: Delegate the entire fetch-and-set logic to CACHE.FETCH.
        // `CACHE.FETCH` already contains the necessary cache stampede protection.
        let fetch_cmd = CacheFetch {
            key: self.key.clone(),
            url: resolved_url
                .ok_or_else(|| SpinelDBError::Internal("URL could not be resolved".into()))?,
            ttl: resolved_ttl,
            swr: resolved_swr,
            grace: resolved_grace,
            tags: resolved_tags,
            vary: self.vary.clone(),
            headers: self.headers.clone(),
        };

        // This call will perform the fetch, store the result (in-memory or on-disk),
        // and return the body for this initial client.
        let (body, _) = fetch_cmd.execute(ctx).await?;
        Ok(RouteResponse::Single(body))
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
        // This command can result in a write, so it's marked as such.
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
        // generated by the fetch is propagated instead, making replication deterministic.
        vec![]
    }
}
