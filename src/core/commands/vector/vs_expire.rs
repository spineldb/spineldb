// src/core/commands/vector/vs_expire.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant};

/// Implements the `VS.EXPIRE` command to set TTL on a vector index.
///
/// VS.EXPIRE key seconds
#[derive(Debug, Clone, Default)]
pub struct VsExpire {
    pub key: Bytes,
    pub seconds: u64,
}

impl ParseCommand for VsExpire {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 2 {
            return Err(SpinelDBError::WrongArgumentCount("VS.EXPIRE".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let seconds = extract_string(&args[1])?
            .parse::<u64>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        if seconds == 0 {
            return Err(SpinelDBError::InvalidRequest(
                "TTL must be greater than 0".to_string(),
            ));
        }

        Ok(VsExpire { key, seconds })
    }
}

#[async_trait]
impl ExecutableCommand for VsExpire {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            sv.set_ttl(self.seconds);
            // Also set StoredValue expiry so the lazy-free system actually deletes the key
            entry.expiry = Some(Instant::now() + Duration::from_secs(self.seconds));
            entry.version = entry.version.wrapping_add(1);

            Ok((
                RespValue::Integer(1),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsExpire {
    fn name(&self) -> &'static str {
        "vs.expire"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM
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
        vec![self.key.clone(), Bytes::from(self.seconds.to_string())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_vsexpire_parse_valid() {
        let c = VsExpire::parse(&[bs("idx"), bs("3600")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.seconds, 3600);
    }

    #[test]
    fn test_vsexpire_too_few_args() {
        let r = VsExpire::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsexpire_zero_is_error() {
        let r = VsExpire::parse(&[bs("idx"), bs("0")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_vsexpire_to_resp_args() {
        let c = VsExpire::parse(&[bs("idx"), bs("3600")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"idx"));
        assert_eq!(args[1], Bytes::from_static(b"3600"));
    }
}
