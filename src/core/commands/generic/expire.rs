// src/core/commands/generic/expire.rs

use super::expire_variants::set_expiry;
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

#[derive(Debug, Clone, Default)]
pub struct Expire {
    pub key: Bytes,
    pub seconds: u64,
}
impl ParseCommand for Expire {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "EXPIRE")?;
        Ok(Expire {
            key: extract_bytes(&args[0])?,
            seconds: extract_string(&args[1])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for Expire {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let expiry = Instant::now() + Duration::from_secs(self.seconds);
        set_expiry(&self.key, Some(expiry), ctx).await
    }
}
impl CommandSpec for Expire {
    fn name(&self) -> &'static str {
        "expire"
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
        vec![self.key.clone(), self.seconds.to_string().into()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_expire_parses_key_and_seconds() {
        let e = Expire::parse(&[bs("k"), bs("30")]).unwrap();
        assert_eq!(e.key, Bytes::from_static(b"k"));
        assert_eq!(e.seconds, 30);
    }

    #[test]
    fn test_expire_parses_zero_seconds() {
        let e = Expire::parse(&[bs("k"), bs("0")]).unwrap();
        assert_eq!(e.seconds, 0);
    }

    #[test]
    fn test_expire_with_too_few_args_is_error() {
        let r = Expire::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_expire_with_too_many_args_is_error() {
        let r = Expire::parse(&[bs("k"), bs("10"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_expire_with_non_integer_seconds_is_error() {
        let r = Expire::parse(&[bs("k"), bs("thirty")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_expire_with_negative_seconds_is_error() {
        // u64 cannot parse negative values.
        let r = Expire::parse(&[bs("k"), bs("-1")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_expire_to_resp_args_round_trips() {
        let e = Expire::parse(&[bs("k"), bs("60")]).unwrap();
        let args = e.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"60"));
    }
}
