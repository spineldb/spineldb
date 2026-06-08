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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &'static str) -> RespFrame {
        RespFrame::BulkString(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn test_cache_lock_parse_lock() -> Result<(), SpinelDBError> {
        let c = CacheLock::parse(&[bs("lock"), bs("key"), bs("60")]).unwrap();
        if let CacheLockSubcommand::Lock { key, ttl_seconds } = &c.subcommand {
            assert_eq!(key.as_ref(), b"key");
            assert_eq!(*ttl_seconds, 60);
        } else {
            panic!("Expected Lock subcommand");
        }
        Ok(())
    }

    #[test]
    fn test_cache_lock_parse_unlock() -> Result<(), SpinelDBError> {
        let c = CacheLock::parse(&[bs("unlock"), bs("key")]).unwrap();
        if let CacheLockSubcommand::Unlock(k) = &c.subcommand {
            assert_eq!(k.as_ref(), b"key");
        } else {
            panic!("Expected Unlock subcommand");
        }
        Ok(())
    }

    #[test]
    fn test_cache_lock_parse_unlock_case_insensitive() -> Result<(), SpinelDBError> {
        let c = CacheLock::parse(&[bs("UNLOCK"), bs("key")]).unwrap();
        assert!(matches!(c.subcommand, CacheLockSubcommand::Unlock(_)));
        Ok(())
    }

    #[test]
    fn test_cache_lock_parse_lock_missing_ttl() -> Result<(), SpinelDBError> {
        let r = CacheLock::parse(&[bs("lock"), bs("key")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
        Ok(())
    }

    #[test]
    fn test_cache_lock_parse_unlock_missing_key() -> Result<(), SpinelDBError> {
        let r = CacheLock::parse(&[bs("unlock")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
        Ok(())
    }

    #[test]
    fn test_cache_lock_parse_no_args() {
        let r = CacheLock::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_cache_lock_parse_unknown_subcommand() {
        let r = CacheLock::parse(&[bs("unknown"), bs("key")]);
        assert!(matches!(r, Err(SpinelDBError::UnknownCommand(_))));
    }

    #[test]
    fn test_cache_lock_lock_invalid_ttl() {
        let r = CacheLock::parse(&[bs("lock"), bs("key"), bs("not_a_number")]);
        assert!(r.is_err());
    }
}
