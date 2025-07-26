// src/core/commands/cache/command.rs

//! The main dispatcher for all `CACHE.*` subcommands.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

// Import the concrete implementations for each subcommand.
use super::cache_bypass::CacheBypass;
use super::cache_fetch::CacheFetch;
use super::cache_get::CacheGet;
use super::cache_info::CacheInfo;
use super::cache_lock::CacheLock;
use super::cache_policy::CachePolicyCmd;
use super::cache_proxy::CacheProxy;
use super::cache_purge::CachePurge;
use super::cache_purgetag::CachePurgeTag;
use super::cache_set::CacheSet;
use super::cache_stats::CacheStats;

/// Enum to hold all possible parsed `CACHE` subcommands.
#[derive(Debug, Clone)]
pub enum CacheSubcommand {
    Set(CacheSet),
    Get(CacheGet),
    PurgeTag(CachePurgeTag),
    Fetch(CacheFetch),
    Stats(CacheStats),
    Proxy(CacheProxy),
    Policy(CachePolicyCmd),
    Purge(CachePurge),
    Lock(CacheLock),
    Unlock(CacheLock),
    Bypass(CacheBypass),
    Info(CacheInfo),
}

/// The main `Cache` command struct that holds a specific subcommand.
/// This acts as the top-level entry point for `CACHE.*` commands.
#[derive(Debug, Clone)]
pub struct Cache {
    pub subcommand: CacheSubcommand,
}

impl Default for Cache {
    /// Provides a default variant. Required for the `get_all_command_specs` function.
    fn default() -> Self {
        Self {
            subcommand: CacheSubcommand::Get(CacheGet::default()),
        }
    }
}

impl ParseCommand for Cache {
    /// Parses the initial RESP frame array to determine which `CACHE` subcommand to use.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CACHE".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let command_args = &args[1..];

        // Delegate parsing to the specific subcommand's implementation.
        let subcommand = match sub_str.as_str() {
            "set" => CacheSubcommand::Set(CacheSet::parse(command_args)?),
            "get" => CacheSubcommand::Get(CacheGet::parse(command_args)?),
            "purgetag" => CacheSubcommand::PurgeTag(CachePurgeTag::parse(command_args)?),
            "fetch" => CacheSubcommand::Fetch(CacheFetch::parse(command_args)?),
            "stats" => CacheSubcommand::Stats(CacheStats::parse(command_args)?),
            "proxy" => CacheSubcommand::Proxy(CacheProxy::parse(command_args)?),
            "policy" => CacheSubcommand::Policy(CachePolicyCmd::parse(command_args)?),
            "purge" => CacheSubcommand::Purge(CachePurge::parse(command_args)?),
            "lock" => CacheSubcommand::Lock(CacheLock::parse(&[
                RespFrame::BulkString(sub_str.into()),
                command_args[0].clone(),
                command_args[1].clone(),
            ])?),
            "unlock" => CacheSubcommand::Unlock(CacheLock::parse(&[
                RespFrame::BulkString(sub_str.into()),
                command_args[0].clone(),
            ])?),
            "bypass" => CacheSubcommand::Bypass(CacheBypass::parse(command_args)?),
            "info" => CacheSubcommand::Info(CacheInfo::parse(command_args)?),
            _ => return Err(SpinelDBError::UnknownCommand(format!("CACHE {sub_str}"))),
        };

        Ok(Cache { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Cache {
    /// Dispatches execution to the specific subcommand's implementation.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            CacheSubcommand::Set(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Get(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::PurgeTag(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Fetch(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Stats(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Proxy(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Policy(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Purge(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Lock(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Unlock(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Bypass(cmd) => cmd.execute(ctx).await,
            CacheSubcommand::Info(cmd) => cmd.execute(ctx).await,
        }
    }
}

// Implement CommandSpec for the main `Cache` dispatcher
impl CommandSpec for Cache {
    fn name(&self) -> &'static str {
        "cache"
    }

    fn arity(&self) -> i64 {
        -2
    }

    fn flags(&self) -> CommandFlags {
        match &self.subcommand {
            CacheSubcommand::Set(cmd) => cmd.flags(),
            CacheSubcommand::Get(cmd) => cmd.flags(),
            CacheSubcommand::PurgeTag(cmd) => cmd.flags(),
            CacheSubcommand::Fetch(cmd) => cmd.flags(),
            CacheSubcommand::Stats(cmd) => cmd.flags(),
            CacheSubcommand::Proxy(cmd) => cmd.flags(),
            CacheSubcommand::Policy(cmd) => cmd.flags(),
            CacheSubcommand::Purge(cmd) => cmd.flags(),
            CacheSubcommand::Lock(cmd) => cmd.flags(),
            CacheSubcommand::Unlock(cmd) => cmd.flags(),
            CacheSubcommand::Bypass(cmd) => cmd.flags(),
            CacheSubcommand::Info(cmd) => cmd.flags(),
        }
    }

    fn first_key(&self) -> i64 {
        match &self.subcommand {
            CacheSubcommand::Set(_)
            | CacheSubcommand::Get(_)
            | CacheSubcommand::Fetch(_)
            | CacheSubcommand::Proxy(_)
            | CacheSubcommand::Bypass(_)
            | CacheSubcommand::Info(_) => 2,
            CacheSubcommand::Lock(cmd) | CacheSubcommand::Unlock(cmd) => cmd.first_key(),
            CacheSubcommand::PurgeTag(_)
            | CacheSubcommand::Stats(_)
            | CacheSubcommand::Policy(_)
            | CacheSubcommand::Purge(_) => 0,
        }
    }

    fn last_key(&self) -> i64 {
        match &self.subcommand {
            CacheSubcommand::Set(_)
            | CacheSubcommand::Get(_)
            | CacheSubcommand::Fetch(_)
            | CacheSubcommand::Proxy(_)
            | CacheSubcommand::Bypass(_)
            | CacheSubcommand::Info(_) => 2,
            CacheSubcommand::Lock(cmd) | CacheSubcommand::Unlock(cmd) => cmd.last_key(),
            _ => 0,
        }
    }

    fn step(&self) -> i64 {
        match &self.subcommand {
            CacheSubcommand::Set(_)
            | CacheSubcommand::Get(_)
            | CacheSubcommand::Fetch(_)
            | CacheSubcommand::Proxy(_)
            | CacheSubcommand::Bypass(_)
            | CacheSubcommand::Info(_) => 1,
            CacheSubcommand::Lock(cmd) | CacheSubcommand::Unlock(cmd) => cmd.step(),
            _ => 0,
        }
    }

    fn get_keys(&self) -> Vec<Bytes> {
        match &self.subcommand {
            CacheSubcommand::Set(cmd) => cmd.get_keys(),
            CacheSubcommand::Get(cmd) => cmd.get_keys(),
            CacheSubcommand::PurgeTag(cmd) => cmd.get_keys(),
            CacheSubcommand::Fetch(cmd) => cmd.get_keys(),
            CacheSubcommand::Stats(cmd) => cmd.get_keys(),
            CacheSubcommand::Proxy(cmd) => cmd.get_keys(),
            CacheSubcommand::Policy(cmd) => cmd.get_keys(),
            CacheSubcommand::Purge(cmd) => cmd.get_keys(),
            CacheSubcommand::Lock(cmd) => cmd.get_keys(),
            CacheSubcommand::Unlock(cmd) => cmd.get_keys(),
            CacheSubcommand::Bypass(cmd) => cmd.get_keys(),
            CacheSubcommand::Info(cmd) => cmd.get_keys(),
        }
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        match &self.subcommand {
            CacheSubcommand::Set(cmd) => {
                let mut args = vec![Bytes::from_static(b"SET")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Get(cmd) => {
                let mut args = vec![Bytes::from_static(b"GET")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::PurgeTag(cmd) => {
                let mut args = vec![Bytes::from_static(b"PURGETAG")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Fetch(cmd) => {
                let mut args = vec![Bytes::from_static(b"FETCH")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Stats(cmd) => {
                let mut args = vec![Bytes::from_static(b"STATS")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Proxy(cmd) => {
                let mut args = vec![Bytes::from_static(b"PROXY")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Policy(cmd) => {
                let mut args = vec![Bytes::from_static(b"POLICY")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Purge(cmd) => {
                let mut args = vec![Bytes::from_static(b"PURGE")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Lock(cmd) => {
                let mut args = vec![Bytes::from_static(b"LOCK")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Unlock(cmd) => {
                let mut args = vec![Bytes::from_static(b"UNLOCK")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Bypass(cmd) => {
                let mut args = vec![Bytes::from_static(b"BYPASS")];
                args.extend(cmd.to_resp_args());
                args
            }
            CacheSubcommand::Info(cmd) => {
                let mut args = vec![Bytes::from_static(b"INFO")];
                args.extend(cmd.to_resp_args());
                args
            }
        }
    }
}
