// src/core/commands/cache/cache_bypass.rs

//! Implements the `CACHE.BYPASS` command, which fetches content directly from an
//! origin without reading from or writing to the cache.

use crate::core::commands::cache::cache_fetch::{CacheFetch, FetchOutcome};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use url::Url;

/// Represents the `CACHE.BYPASS` command.
#[derive(Debug, Clone, Default)]
pub struct CacheBypass {
    pub key: Bytes,
    pub url: String,
}

impl ParseCommand for CacheBypass {
    /// Parses the command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 2 {
            return Err(SpinelDBError::WrongArgumentCount(
                "CACHE.BYPASS".to_string(),
            ));
        }
        Ok(CacheBypass {
            key: extract_bytes(&args[0])?,
            url: extract_string(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for CacheBypass {
    /// Executes the `CACHE.BYPASS` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // DNS Rebinding Fix: Pre-resolve URL and pass IP to fetcher.
        let (allowed_domains, allow_private) = {
            let config = ctx.state.config.lock().await;
            (
                config.security.allowed_fetch_domains.clone(),
                config.security.allow_private_fetch_ips,
            )
        };
        let resolved_ips = crate::core::commands::helpers::validate_fetch_url(
            &self.url,
            &allowed_domains,
            allow_private,
        )
        .await?;
        let target_ip = resolved_ips.first().cloned().ok_or_else(|| {
            SpinelDBError::Internal("Validated URL did not return any IP addresses".to_string())
        })?;
        let url_parsed = Url::parse(&self.url)
            .map_err(|e| SpinelDBError::InvalidRequest(format!("Invalid URL: {e}")))?;
        let domain = url_parsed.host_str().unwrap_or("").to_string();

        // Construct a `CacheFetch` command to delegate the origin fetch logic.
        let fetch_cmd = CacheFetch {
            key: self.key.clone(),
            url: self.url.clone(),
            ..Default::default()
        };

        // Call the fetch logic with the `bypass_store` flag set to true.
        let (outcome, _) = fetch_cmd
            .fetch_from_origin(&ctx.state, true, target_ip, domain)
            .await?;

        // Convert the fetch outcome into a single byte buffer for the client.
        let body_bytes = match outcome {
            FetchOutcome::InMemory(bytes) => bytes,
            FetchOutcome::OnDisk { path, .. } => tokio::fs::read(&path).await?.into(),
            FetchOutcome::Negative { status, body } => {
                // If the bypassed fetch resulted in an error, propagate it to the client.
                return Err(SpinelDBError::InvalidState(format!(
                    "Origin responded with status {status}: {}",
                    String::from_utf8_lossy(&body.unwrap_or_default())
                )));
            }
        };

        Ok((RespValue::BulkString(body_bytes), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for CacheBypass {
    fn name(&self) -> &'static str {
        "cache.bypass"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::NO_PROPAGATE | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone(), self.url.clone().into()]
    }
}
