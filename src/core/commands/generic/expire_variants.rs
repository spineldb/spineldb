// src/core/commands/generic/expire_variants.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// --- Helper Logic Bersama untuk semua perintah expiry ---
pub async fn set_expiry<'a>(
    key: &Bytes,
    expiry: Option<Instant>,
    ctx: &mut ExecutionContext<'a>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let shard_cache_guard = match &mut ctx.locks {
        ExecutionLocks::Single { guard, .. } => guard,
        ExecutionLocks::Multi { guards } => {
            let shard_index = ctx.db.get_shard_index(key);
            guards.get_mut(&shard_index).ok_or_else(|| {
                SpinelDBError::Internal("Mismatched lock in multi-key command for expiry".into())
            })?
        }
        _ => {
            return Err(SpinelDBError::Internal(
                "Expiry command requires a lock".into(),
            ));
        }
    };

    if let Some(entry) = shard_cache_guard.get_mut(key) {
        if entry.is_expired() {
            // Key ada tapi sudah kedaluwarsa, jadi dianggap tidak ada.
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        entry.expiry = expiry;
        entry.version = entry.version.wrapping_add(1);
        Ok((
            RespValue::Integer(1),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    } else {
        // Key tidak ditemukan
        Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
    }
}

// --- PEXPIRE ---

#[derive(Debug, Clone, Default)]
pub struct PExpire {
    pub key: Bytes,
    pub milliseconds: u64,
}
impl ParseCommand for PExpire {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "PEXPIRE")?;
        Ok(PExpire {
            key: extract_bytes(&args[0])?,
            milliseconds: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for PExpire {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let expiry = Instant::now() + Duration::from_millis(self.milliseconds);
        set_expiry(&self.key, Some(expiry), ctx).await
    }
}
impl CommandSpec for PExpire {
    fn name(&self) -> &'static str {
        "pexpire"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone(), self.milliseconds.to_string().into()]
    }
}

// --- EXPIREAT ---

#[derive(Debug, Clone, Default)]
pub struct ExpireAt {
    pub key: Bytes,
    pub unix_seconds: u64,
}
impl ParseCommand for ExpireAt {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "EXPIREAT")?;
        Ok(ExpireAt {
            key: extract_bytes(&args[0])?,
            unix_seconds: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ExpireAt {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let target_time = UNIX_EPOCH + Duration::from_secs(self.unix_seconds);
        let expiry = if let Ok(duration_from_now) = target_time.duration_since(SystemTime::now()) {
            Some(Instant::now() + duration_from_now)
        } else {
            // Waktu sudah lewat, key akan segera dihapus
            Some(Instant::now())
        };
        set_expiry(&self.key, expiry, ctx).await
    }
}
impl CommandSpec for ExpireAt {
    fn name(&self) -> &'static str {
        "expireat"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone(), self.unix_seconds.to_string().into()]
    }
}

// --- PEXPIREAT ---

#[derive(Debug, Clone, Default)]
pub struct PExpireAt {
    pub key: Bytes,
    pub unix_milliseconds: u64,
}
impl ParseCommand for PExpireAt {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "PEXPIREAT")?;
        Ok(PExpireAt {
            key: extract_bytes(&args[0])?,
            unix_milliseconds: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for PExpireAt {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let target_time = UNIX_EPOCH + Duration::from_millis(self.unix_milliseconds);
        let expiry = if let Ok(duration_from_now) = target_time.duration_since(SystemTime::now()) {
            Some(Instant::now() + duration_from_now)
        } else {
            Some(Instant::now())
        };
        set_expiry(&self.key, expiry, ctx).await
    }
}
impl CommandSpec for PExpireAt {
    fn name(&self) -> &'static str {
        "pexpireat"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone(), self.unix_milliseconds.to_string().into()]
    }
}
