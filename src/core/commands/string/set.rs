// src/core/commands/string/set.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_bytes};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Defines the condition for `SET` execution (`NX` or `XX`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SetCondition {
    #[default]
    None, // Always set.
    IfExists,    // `XX` - Only set if the key already exists.
    IfNotExists, // `NX` - Only set if the key does not already exist.
}

/// Defines the TTL options for the `SET` command and its variants.
#[derive(Debug, Clone, Copy, Default)]
pub enum TtlOption {
    #[default]
    None, // No TTL option was provided; will remove existing TTL.
    Seconds(u64),
    Milliseconds(u64),
    UnixSeconds(u64),
    UnixMilliseconds(u64),
    Persist,      // Explicitly remove the TTL.
    KeepExisting, // KEEPTTL flag.
}

/// Represents the full `SET` command with all its options.
#[derive(Debug, Clone, Default)]
pub struct Set {
    pub key: Bytes,
    pub value: Bytes,
    pub ttl: TtlOption,
    pub condition: SetCondition,
    pub get: bool, // `GET` option to return the old value.
}

impl ParseCommand for Set {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("SET".to_string()));
        }
        let mut cmd = Set {
            key: extract_bytes(&args[0])?,
            value: extract_bytes(&args[1])?,
            ..Default::default()
        };

        let mut parser = ArgParser::new(&args[2..]);
        let mut ttl_option_count = 0;

        loop {
            if let Some(seconds) = parser.match_option("ex")? {
                cmd.ttl = TtlOption::Seconds(seconds);
                ttl_option_count += 1;
            } else if let Some(ms) = parser.match_option("px")? {
                cmd.ttl = TtlOption::Milliseconds(ms);
                ttl_option_count += 1;
            } else if let Some(ts_secs) = parser.match_option("exat")? {
                cmd.ttl = TtlOption::UnixSeconds(ts_secs);
                ttl_option_count += 1;
            } else if let Some(ts_ms) = parser.match_option("pxat")? {
                cmd.ttl = TtlOption::UnixMilliseconds(ts_ms);
                ttl_option_count += 1;
            } else if parser.match_flag("keepttl") {
                cmd.ttl = TtlOption::KeepExisting;
                ttl_option_count += 1;
            } else if parser.match_flag("nx") {
                if cmd.condition != SetCondition::None {
                    return Err(SpinelDBError::SyntaxError);
                }
                cmd.condition = SetCondition::IfNotExists;
            } else if parser.match_flag("xx") {
                if cmd.condition != SetCondition::None {
                    return Err(SpinelDBError::SyntaxError);
                }
                cmd.condition = SetCondition::IfExists;
            } else if parser.match_flag("get") {
                cmd.get = true;
            } else {
                break;
            }
        }

        if ttl_option_count > 1 {
            return Err(SpinelDBError::SyntaxError);
        }
        if !parser.remaining_args().is_empty() {
            return Err(SpinelDBError::SyntaxError);
        }
        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for Set {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Pre-fetch the old value if GET is specified. This is done before any modifications.
        let old_value_for_get = if self.get {
            shard_cache_guard
                .peek(&self.key)
                .and_then(|entry| {
                    if entry.is_expired() {
                        None
                    } else if let DataValue::String(s) = &entry.data {
                        Some(RespValue::BulkString(s.clone()))
                    } else {
                        // If the key exists but is not a string, GET returns an error later.
                        // Here, for the purpose of returning the old value, we treat it as if it's a type mismatch.
                        // The actual WRONGTYPE check happens next.
                        None
                    }
                })
                .unwrap_or(RespValue::Null)
        } else {
            RespValue::Null
        };

        // Pre-check for WRONGTYPE.
        let key_exists_and_is_valid = if let Some(entry) = shard_cache_guard.peek(&self.key) {
            if entry.is_expired() {
                false
            } else if matches!(entry.data, DataValue::String(_)) {
                true
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            false
        };

        // Check conditions (NX/XX) and abort if they are not met.
        if (self.condition == SetCondition::IfExists && !key_exists_and_is_valid)
            || (self.condition == SetCondition::IfNotExists && key_exists_and_is_valid)
        {
            return Ok((
                if self.get {
                    old_value_for_get
                } else {
                    RespValue::Null
                },
                WriteOutcome::DidNotWrite,
            ));
        }

        // Calculate the new expiry time based on the provided TTL option.
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
            TtlOption::Persist => None,
            TtlOption::KeepExisting => {
                // Only keep TTL if the key exists and is not expired.
                if key_exists_and_is_valid {
                    shard_cache_guard.peek(&self.key).and_then(|e| e.expiry)
                } else {
                    None // Otherwise, the new key has no TTL.
                }
            }
            TtlOption::None => None, // Default SET behavior removes any existing TTL.
        };

        // If the calculated expiry is in the past, the key is effectively deleted.
        if new_expiry.is_some_and(|exp| exp <= Instant::now()) {
            let existed_before = shard_cache_guard.pop(&self.key).is_some();
            let response = if self.get {
                old_value_for_get
            } else {
                RespValue::SimpleString("OK".into())
            };
            let outcome = if existed_before {
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::DidNotWrite
            };
            return Ok((response, outcome));
        }

        // Proceed with setting the key.
        let mut new_stored_value = StoredValue::new(DataValue::String(self.value.clone()));
        new_stored_value.expiry = new_expiry;

        // Preserve version for WATCH command correctness.
        if key_exists_and_is_valid && let Some(old_entry) = shard_cache_guard.peek(&self.key) {
            new_stored_value.version = old_entry.version.wrapping_add(1);
        }

        shard_cache_guard.put(self.key.clone(), new_stored_value);

        let response = if self.get {
            old_value_for_get
        } else {
            RespValue::SimpleString("OK".into())
        };

        Ok((response, WriteOutcome::Write { keys_modified: 1 }))
    }
}

impl CommandSpec for Set {
    fn name(&self) -> &'static str {
        "set"
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
        let mut args = vec![self.key.clone(), self.value.clone()];
        match self.ttl {
            TtlOption::Seconds(ttl) => {
                args.extend([Bytes::from_static(b"EX"), ttl.to_string().into()])
            }
            TtlOption::Milliseconds(ttl) => {
                args.extend([Bytes::from_static(b"PX"), ttl.to_string().into()])
            }
            TtlOption::UnixSeconds(ttl) => {
                args.extend([Bytes::from_static(b"EXAT"), ttl.to_string().into()])
            }
            TtlOption::UnixMilliseconds(ttl) => {
                args.extend([Bytes::from_static(b"PXAT"), ttl.to_string().into()])
            }
            TtlOption::KeepExisting => args.push(Bytes::from_static(b"KEEPTTL")),
            // PERSIST is a valid SET option, although less common than `PERSIST key`.
            // It means remove the TTL. This is the default behavior if no TTL option is given,
            // so we only need to serialize it if it was explicitly provided.
            TtlOption::Persist => args.push(Bytes::from_static(b"PERSIST")),
            TtlOption::None => {}
        }
        if self.condition == SetCondition::IfNotExists {
            args.push("NX".into());
        }
        if self.condition == SetCondition::IfExists {
            args.push("XX".into());
        }
        if self.get {
            args.push("GET".into());
        }
        args
    }
}
