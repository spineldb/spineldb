// src/core/commands/string/set.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{ArgParser, extract_bytes};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::hll::HyperLogLog;
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
#[derive(Debug, Clone, Copy, PartialEq, Default)]
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

        // Determine the type of the value being set by checking for HLL magic header.
        let new_data_value = if let Some(hll) = HyperLogLog::deserialize(&self.value) {
            DataValue::HyperLogLog(Box::new(hll))
        } else {
            DataValue::String(self.value.clone())
        };

        // Perform WRONGTYPE check. The existing key's type must match the new value's type.
        let key_exists_and_is_valid = if let Some(entry) = shard_cache_guard.peek(&self.key) {
            if entry.is_expired() {
                false
            } else {
                match (&new_data_value, &entry.data) {
                    (DataValue::String(_), DataValue::String(_)) => true,
                    (DataValue::HyperLogLog(_), DataValue::HyperLogLog(_)) => true,
                    // Any other combination is a WRONGTYPE error.
                    _ => return Err(SpinelDBError::WrongType),
                }
            }
        } else {
            false // Key does not exist.
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
        let mut new_stored_value = StoredValue::new(new_data_value);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_set_parse_basic() {
        let c = Set::parse(&[bs("k"), bs("v")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.value, Bytes::from_static(b"v"));
        assert_eq!(c.ttl, TtlOption::None);
        assert_eq!(c.condition, SetCondition::None);
        assert!(!c.get);
    }

    #[test]
    fn test_set_parse_ex() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("EX"), bs("10")]).unwrap();
        assert_eq!(c.ttl, TtlOption::Seconds(10));
    }

    #[test]
    fn test_set_parse_px() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("PX"), bs("1000")]).unwrap();
        assert_eq!(c.ttl, TtlOption::Milliseconds(1000));
    }

    #[test]
    fn test_set_parse_exat() {
        let ts: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let ts_bytes = Bytes::from(ts.to_string());
        let c = Set::parse(&[
            bs("k"),
            bs("v"),
            bs("EXAT"),
            RespFrame::BulkString(ts_bytes),
        ])
        .unwrap();
        if let TtlOption::UnixSeconds(seconds) = c.ttl {
            assert_eq!(seconds, ts);
        } else {
            panic!("Expected UnixSeconds");
        }
    }

    #[test]
    fn test_set_parse_pxat() {
        let ts: u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 5000;
        let ts_bytes = Bytes::from(ts.to_string());
        let c = Set::parse(&[
            bs("k"),
            bs("v"),
            bs("PXAT"),
            RespFrame::BulkString(ts_bytes),
        ])
        .unwrap();
        if let TtlOption::UnixMilliseconds(ms) = c.ttl {
            assert_eq!(ms as u128, ts);
        } else {
            panic!("Expected UnixMilliseconds");
        }
    }

    #[test]
    fn test_set_parse_keepttl() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("KEEPTTL")]).unwrap();
        assert_eq!(c.ttl, TtlOption::KeepExisting);
    }

    #[test]
    fn test_set_parse_nx() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("NX")]).unwrap();
        assert_eq!(c.condition, SetCondition::IfNotExists);
    }

    #[test]
    fn test_set_parse_xx() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("XX")]).unwrap();
        assert_eq!(c.condition, SetCondition::IfExists);
    }

    #[test]
    fn test_set_parse_get() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("GET")]).unwrap();
        assert!(c.get);
    }

    #[test]
    fn test_set_parse_combined_options() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("NX"), bs("EX"), bs("10")]).unwrap();
        assert_eq!(c.condition, SetCondition::IfNotExists);
        assert_eq!(c.ttl, TtlOption::Seconds(10));
    }

    #[test]
    fn test_set_parse_too_few_args() {
        let r = Set::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_set_parse_empty_args() {
        let r = Set::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_set_parse_multiple_ttl_options_is_error() {
        let r = Set::parse(&[bs("k"), bs("v"), bs("EX"), bs("10"), bs("PX"), bs("100")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_set_parse_both_nx_and_xx_is_error() {
        let r = Set::parse(&[bs("k"), bs("v"), bs("NX"), bs("XX")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_set_parse_unknown_option_is_error() {
        let r = Set::parse(&[bs("k"), bs("v"), bs("UNKNOWN")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_set_to_resp_args_basic() {
        let c = Set::parse(&[bs("k"), bs("v")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"v"));
    }

    #[test]
    fn test_set_to_resp_args_with_ex() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("EX"), bs("10")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[2], Bytes::from_static(b"EX"));
        assert_eq!(args[3], Bytes::from_static(b"10"));
    }

    #[test]
    fn test_set_to_resp_args_with_exat() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("EXAT"), bs("1234567890")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[2], Bytes::from_static(b"EXAT"));
        assert_eq!(args[3], Bytes::from_static(b"1234567890"));
    }

    #[test]
    fn test_set_to_resp_args_with_keepttl() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("KEEPTTL")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[2], Bytes::from_static(b"KEEPTTL"));
    }

    #[test]
    fn test_set_to_resp_args_with_nx() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("NX")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[2], Bytes::from_static(b"NX"));
    }

    #[test]
    fn test_set_to_resp_args_with_xx() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("XX")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[2], Bytes::from_static(b"XX"));
    }

    #[test]
    fn test_set_to_resp_args_with_get() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("GET")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[2], Bytes::from_static(b"GET"));
    }

    #[test]
    fn test_set_to_resp_args_combined() {
        let c = Set::parse(&[bs("k"), bs("v"), bs("NX"), bs("EX"), bs("10"), bs("GET")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 6);
        // Order is: key, value, EX, 10 (TTL), NX (condition), GET (flag)
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"v"));
        assert_eq!(args[2], Bytes::from_static(b"EX"));
        assert_eq!(args[3], Bytes::from_static(b"10"));
        assert_eq!(args[4], Bytes::from_static(b"NX"));
        assert_eq!(args[5], Bytes::from_static(b"GET"));
    }

    #[test]
    fn test_set_ttl_serialization() {
        let c = Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            ttl: TtlOption::Seconds(30),
            ..Default::default()
        };
        let args = c.to_resp_args();
        assert_eq!(args[2], Bytes::from_static(b"EX"));
        assert_eq!(args[3], Bytes::from_static(b"30"));
    }

    #[test]
    fn test_set_px_ttl_serialization() {
        let c = Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            ttl: TtlOption::Milliseconds(500),
            ..Default::default()
        };
        let args = c.to_resp_args();
        assert_eq!(args[2], Bytes::from_static(b"PX"));
        assert_eq!(args[3], Bytes::from_static(b"500"));
    }
}
