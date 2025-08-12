// src/core/commands/cache/cache_policy.rs

//! Implements the `CACHE.POLICY` command family for managing declarative caching rules.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::cache_types::CachePolicy;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use tracing::debug;
use wildmatch::WildMatch;

/// Defines the subcommands for `CACHE.POLICY`.
#[derive(Debug, Clone)]
pub enum CachePolicySubcommand {
    /// Sets or updates a caching policy.
    Set(Box<CachePolicy>),
    /// Deletes a caching policy by name.
    Del(String),
    /// Retrieves the configuration of a specific policy.
    Get(String),
    /// Lists the names of all configured policies.
    List,
}

/// The main command struct for `CACHE.POLICY`. It dispatches to subcommands.
#[derive(Debug, Clone)]
pub struct CachePolicyCmd {
    pub subcommand: CachePolicySubcommand,
}

impl Default for CachePolicyCmd {
    fn default() -> Self {
        Self {
            subcommand: CachePolicySubcommand::List,
        }
    }
}

impl ParseCommand for CachePolicyCmd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "CACHE.POLICY".to_string(),
            ));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let command_args = &args[1..];

        let subcommand = match sub_str.as_str() {
            "set" => {
                if command_args.len() < 3 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CACHE.POLICY SET".to_string(),
                    ));
                }
                let mut policy = CachePolicy {
                    name: extract_string(&command_args[0])?,
                    key_pattern: extract_string(&command_args[1])?,
                    url_template: extract_string(&command_args[2])?,
                    ttl: None,
                    swr: None,
                    grace: None,
                    tags: vec![],
                    prewarm: false,
                    disallow_status_codes: vec![],
                    max_size_bytes: None,
                    vary_on: vec![],
                    respect_origin_headers: false,
                    negative_ttl: None,
                    priority: 0,
                    compression: false,
                    force_disk: false,
                };

                let mut parser = ArgParser::new(&command_args[3..]);
                let mut tags_found = false;
                let mut vary_on_found = false;

                while !parser.remaining_args().is_empty() {
                    if tags_found || vary_on_found {
                        break;
                    }
                    if let Some(v) = parser.match_option("ttl")? {
                        policy.ttl = Some(v);
                    } else if let Some(v) = parser.match_option("swr")? {
                        policy.swr = Some(v);
                    } else if let Some(v) = parser.match_option("grace")? {
                        policy.grace = Some(v);
                    } else if let Some(v) = parser.match_option("negative_ttl")? {
                        policy.negative_ttl = Some(v);
                    } else if let Some(v) = parser.match_option("priority")? {
                        policy.priority = v;
                    } else if parser.match_flag("compression") {
                        policy.compression = true;
                    } else if parser.match_flag("force-disk") {
                        policy.force_disk = true;
                    } else if parser.match_flag("prewarm") {
                        policy.prewarm = true;
                    } else if parser.match_flag("respect_origin_headers") {
                        policy.respect_origin_headers = true;
                    } else if parser.match_flag("tags") {
                        tags_found = true;
                        break;
                    } else if parser.match_flag("vary_on") {
                        vary_on_found = true;
                        break;
                    } else {
                        return Err(SpinelDBError::SyntaxError);
                    }
                }

                if vary_on_found {
                    policy.vary_on = parser
                        .remaining_args()
                        .iter()
                        .map(extract_string)
                        .collect::<Result<_, _>>()?;
                } else if tags_found {
                    policy.tags = parser
                        .remaining_args()
                        .iter()
                        .map(extract_string)
                        .collect::<Result<_, _>>()?;
                }

                CachePolicySubcommand::Set(Box::new(policy))
            }
            "del" => {
                if command_args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CACHE.POLICY DEL".to_string(),
                    ));
                }
                CachePolicySubcommand::Del(extract_string(&command_args[0])?)
            }
            "get" => {
                if command_args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CACHE.POLICY GET".to_string(),
                    ));
                }
                CachePolicySubcommand::Get(extract_string(&command_args[0])?)
            }
            "list" => {
                if !command_args.is_empty() {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CACHE.POLICY LIST".to_string(),
                    ));
                }
                CachePolicySubcommand::List
            }
            _ => {
                return Err(SpinelDBError::UnknownCommand(format!(
                    "CACHE.POLICY {sub_str}"
                )));
            }
        };

        Ok(Self { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for CachePolicyCmd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            CachePolicySubcommand::Set(policy_to_set) => {
                let mut policies = ctx.state.cache.policies.write().await;
                let old_policy = policies
                    .iter()
                    .find(|p| p.name == policy_to_set.name)
                    .cloned();

                if let Some(existing) = policies.iter_mut().find(|p| p.name == policy_to_set.name) {
                    *existing = (**policy_to_set).clone();
                } else {
                    policies.push((**policy_to_set).clone());
                }
                // Re-sort policies by priority after any modification.
                policies.sort_by_key(|p| std::cmp::Reverse(p.priority));
                drop(policies);

                // If the `prewarm` flag changed, update the prewarm key set.
                if let Some(old) = old_policy
                    && old.prewarm
                    && !policy_to_set.prewarm
                {
                    debug!(
                        "Policy '{}' changed from prewarm=true to false. Cleaning up prewarm keys.",
                        old.name
                    );
                    let mut prewarm_keys = ctx.state.cache.prewarm_keys.write().await;
                    let matcher = WildMatch::new(&old.key_pattern);
                    prewarm_keys.retain(|key| !matcher.matches(&String::from_utf8_lossy(key)));
                }

                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            CachePolicySubcommand::Del(name) => {
                let mut policies = ctx.state.cache.policies.write().await;
                let policy_to_delete = policies.iter().find(|p| p.name == *name).cloned();

                let initial_len = policies.len();
                policies.retain(|p| p.name != *name);
                let removed_count = initial_len - policies.len();
                drop(policies);

                if let Some(deleted_policy) = policy_to_delete
                    && deleted_policy.prewarm
                {
                    debug!(
                        "Prewarm policy '{}' deleted. Cleaning up prewarm keys.",
                        deleted_policy.name
                    );
                    let mut prewarm_keys = ctx.state.cache.prewarm_keys.write().await;
                    let matcher = WildMatch::new(&deleted_policy.key_pattern);
                    prewarm_keys.retain(|key| !matcher.matches(&String::from_utf8_lossy(key)));
                }

                Ok((
                    RespValue::Integer(removed_count as i64),
                    WriteOutcome::DidNotWrite,
                ))
            }
            CachePolicySubcommand::Get(name) => {
                let policies = ctx.state.cache.policies.read().await;
                if let Some(policy) = policies.iter().find(|p| p.name == *name) {
                    let mut info = vec![
                        RespValue::BulkString("name".into()),
                        RespValue::BulkString(policy.name.clone().into()),
                        RespValue::BulkString("key_pattern".into()),
                        RespValue::BulkString(policy.key_pattern.clone().into()),
                        RespValue::BulkString("url_template".into()),
                        RespValue::BulkString(policy.url_template.clone().into()),
                    ];
                    if let Some(v) = policy.ttl {
                        info.push(RespValue::BulkString("ttl".into()));
                        info.push(RespValue::Integer(v as i64));
                    }
                    if let Some(v) = policy.swr {
                        info.push(RespValue::BulkString("swr".into()));
                        info.push(RespValue::Integer(v as i64));
                    }
                    if let Some(v) = policy.grace {
                        info.push(RespValue::BulkString("grace".into()));
                        info.push(RespValue::Integer(v as i64));
                    }
                    if let Some(v) = policy.negative_ttl {
                        info.push(RespValue::BulkString("negative_ttl".into()));
                        info.push(RespValue::Integer(v as i64));
                    }
                    if !policy.tags.is_empty() {
                        info.push(RespValue::BulkString("tags".into()));
                        info.push(RespValue::BulkString(policy.tags.join(" ").into()));
                    }
                    if !policy.vary_on.is_empty() {
                        info.push(RespValue::BulkString("vary_on".into()));
                        info.push(RespValue::BulkString(policy.vary_on.join(" ").into()));
                    }
                    if policy.prewarm {
                        info.push(RespValue::BulkString("prewarm".into()));
                        info.push(RespValue::Integer(1));
                    }
                    if policy.respect_origin_headers {
                        info.push(RespValue::BulkString("respect_origin_headers".into()));
                        info.push(RespValue::Integer(1));
                    }
                    info.push(RespValue::BulkString("priority".into()));
                    info.push(RespValue::Integer(policy.priority as i64));
                    if policy.compression {
                        info.push(RespValue::BulkString("compression".into()));
                        info.push(RespValue::Integer(1));
                    }
                    if policy.force_disk {
                        info.push(RespValue::BulkString("force-disk".into()));
                        info.push(RespValue::Integer(1));
                    }
                    Ok((RespValue::Array(info), WriteOutcome::DidNotWrite))
                } else {
                    Ok((RespValue::Null, WriteOutcome::DidNotWrite))
                }
            }
            CachePolicySubcommand::List => {
                let policies = ctx.state.cache.policies.read().await;
                let list = policies
                    .iter()
                    .map(|p| RespValue::BulkString(p.name.clone().into()))
                    .collect();
                Ok((RespValue::Array(list), WriteOutcome::DidNotWrite))
            }
        }
    }
}

impl CommandSpec for CachePolicyCmd {
    fn name(&self) -> &'static str {
        "cache.policy"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE
    }
    fn first_key(&self) -> i64 {
        0
    }
    fn last_key(&self) -> i64 {
        0
    }
    fn step(&self) -> i64 {
        0
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![]
    }
}
