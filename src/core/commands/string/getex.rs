// File: src/core/commands/string/getex.rs

use super::set::TtlOption;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct GetEx {
    pub key: Bytes,
    pub ttl: TtlOption,
}

impl ParseCommand for GetEx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("GETEX".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let mut ttl = TtlOption::None; // Default is no TTL change

        if args.len() > 1 {
            let option = extract_string(&args[1])?.to_ascii_lowercase();
            // Only one TTL option is allowed
            if args.len() > 3 {
                return Err(SpinelDBError::SyntaxError);
            }
            ttl = match option.as_str() {
                "ex" => {
                    if args.len() != 3 {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let seconds = extract_string(&args[2])?.parse()?;
                    TtlOption::Seconds(seconds)
                }
                "px" => {
                    if args.len() != 3 {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let ms = extract_string(&args[2])?.parse()?;
                    TtlOption::Milliseconds(ms)
                }
                "exat" => {
                    if args.len() != 3 {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let ts_secs = extract_string(&args[2])?.parse()?;
                    TtlOption::UnixSeconds(ts_secs)
                }
                "pxat" => {
                    if args.len() != 3 {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let ts_ms = extract_string(&args[2])?.parse()?;
                    TtlOption::UnixMilliseconds(ts_ms)
                }
                "persist" => {
                    if args.len() != 2 {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    TtlOption::Persist
                }
                // GETEX does not support KEEPTTL, so it's a syntax error.
                _ => return Err(SpinelDBError::SyntaxError),
            };
        }
        Ok(GetEx { key, ttl })
    }
}

#[async_trait]
impl ExecutableCommand for GetEx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Get the value to return, if it exists and is valid.
        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };

        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        let response = match &entry.data {
            DataValue::String(s) => RespValue::BulkString(s.clone()),
            _ => return Err(SpinelDBError::WrongType),
        };

        // Only modify TTL if an option was provided.
        if !matches!(self.ttl, TtlOption::None) {
            let new_expiry = match self.ttl {
                TtlOption::Seconds(s) => Some(Instant::now() + Duration::from_secs(s)),
                TtlOption::Milliseconds(ms) => Some(Instant::now() + Duration::from_millis(ms)),
                TtlOption::UnixSeconds(ts) => {
                    let target_time = UNIX_EPOCH + Duration::from_secs(ts);
                    target_time
                        .duration_since(SystemTime::now())
                        .ok()
                        .map(|d| Instant::now() + d)
                }
                TtlOption::UnixMilliseconds(ts) => {
                    let target_time = UNIX_EPOCH + Duration::from_millis(ts);
                    target_time
                        .duration_since(SystemTime::now())
                        .ok()
                        .map(|d| Instant::now() + d)
                }
                // PERSIST means removing the expiry.
                TtlOption::Persist => None,
                TtlOption::KeepExisting | TtlOption::None => entry.expiry,
            };

            entry.expiry = new_expiry;
            entry.version = entry.version.wrapping_add(1);

            Ok((response, WriteOutcome::Write { keys_modified: 1 }))
        } else {
            // If no TTL option was given, it's a read-only operation.
            Ok((response, WriteOutcome::DidNotWrite))
        }
    }
}

impl CommandSpec for GetEx {
    fn name(&self) -> &'static str {
        "getex"
    }
    fn arity(&self) -> i64 {
        -2
    }
    // Flags are dynamic based on the options provided.
    fn flags(&self) -> CommandFlags {
        if matches!(self.ttl, TtlOption::None) {
            CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
        } else {
            CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
        }
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
        let mut args = vec![self.key.clone()];
        match self.ttl {
            TtlOption::Seconds(ttl) => {
                args.extend([Bytes::from_static(b"EX"), ttl.to_string().into()]);
            }
            TtlOption::Milliseconds(ttl) => {
                args.extend([Bytes::from_static(b"PX"), ttl.to_string().into()]);
            }
            TtlOption::UnixSeconds(ttl) => {
                args.extend([Bytes::from_static(b"EXAT"), ttl.to_string().into()]);
            }
            TtlOption::UnixMilliseconds(ttl) => {
                args.extend([Bytes::from_static(b"PXAT"), ttl.to_string().into()]);
            }
            // Serialize the PERSIST option for AOF/Replication.
            TtlOption::Persist => {
                args.push(Bytes::from_static(b"PERSIST"));
            }

            TtlOption::KeepExisting | TtlOption::None => {}
        }
        args
    }
}
