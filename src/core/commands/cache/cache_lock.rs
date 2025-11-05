// src/core/commands/cache/cache_lock.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum CacheLockSubcommand {
    Lock { key: Bytes, ttl_seconds: u64 },
    Unlock(Bytes),
}

#[derive(Debug, Clone)]
pub struct CacheLock {
    pub subcommand: CacheLockSubcommand,
}

impl Default for CacheLock {
    fn default() -> Self {
        Self {
            subcommand: CacheLockSubcommand::Unlock(Bytes::new()),
        }
    }
}

impl ParseCommand for CacheLock {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "CACHE.LOCK/UNLOCK".into(),
            ));
        }
        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "lock" => {
                validate_arg_count(&args[1..], 2, "CACHE.LOCK")?;
                let key = extract_bytes(&args[1])?;
                let ttl_seconds = extract_string(&args[2])?.parse()?;
                CacheLockSubcommand::Lock { key, ttl_seconds }
            }
            "unlock" => {
                validate_arg_count(&args[1..], 1, "CACHE.UNLOCK")?;
                CacheLockSubcommand::Unlock(extract_bytes(&args[1])?)
            }
            _ => {
                return Err(SpinelDBError::UnknownCommand(
                    "Unknown CACHE.LOCK subcommand".into(),
                ));
            }
        };
        Ok(CacheLock { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for CacheLock {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            CacheLockSubcommand::Lock { key, ttl_seconds } => {
                let expiry = Instant::now() + Duration::from_secs(*ttl_seconds);
                ctx.state.cache.manual_locks.insert(key.clone(), expiry);
                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            CacheLockSubcommand::Unlock(key) => {
                let removed = ctx.state.cache.manual_locks.remove(key).is_some();
                Ok((
                    RespValue::Integer(removed as i64),
                    WriteOutcome::DidNotWrite,
                ))
            }
        }
    }
}

impl CommandSpec for CacheLock {
    fn name(&self) -> &'static str {
        match self.subcommand {
            CacheLockSubcommand::Lock { .. } => "cache.lock",
            CacheLockSubcommand::Unlock(_) => "cache.unlock",
        }
    }
    fn arity(&self) -> i64 {
        match self.subcommand {
            CacheLockSubcommand::Lock { .. } => 4,
            CacheLockSubcommand::Unlock(_) => 2,
        }
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE
    }
    fn first_key(&self) -> i64 {
        2
    }
    fn last_key(&self) -> i64 {
        2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        match &self.subcommand {
            CacheLockSubcommand::Lock { key, .. } => vec![key.clone()],
            CacheLockSubcommand::Unlock(key) => vec![key.clone()],
        }
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        match &self.subcommand {
            CacheLockSubcommand::Lock { key, ttl_seconds } => {
                vec![key.clone(), ttl_seconds.to_string().into()]
            }
            CacheLockSubcommand::Unlock(key) => vec![key.clone()],
        }
    }
}
