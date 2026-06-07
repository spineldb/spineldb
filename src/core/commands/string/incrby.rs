// src/core/commands/string/incrby.rs
use super::incr::do_incr_decr_by;
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

#[derive(Debug, Clone, Default)]
pub struct IncrBy {
    pub key: Bytes,
    pub increment: i64,
}
impl ParseCommand for IncrBy {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "INCRBY")?;
        let increment = extract_string(&args[1])?
            .parse::<i64>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;
        Ok(IncrBy {
            key: extract_bytes(&args[0])?,
            increment,
        })
    }
}
#[async_trait]
impl ExecutableCommand for IncrBy {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        do_incr_decr_by(&self.key, self.increment, ctx).await
    }
}
impl CommandSpec for IncrBy {
    fn name(&self) -> &'static str {
        "incrby"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![self.key.clone(), self.increment.to_string().into()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_incrby_parses_positive_increment() {
        let c = IncrBy::parse(&[bs("counter"), bs("5")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"counter"));
        assert_eq!(c.increment, 5);
    }

    #[test]
    fn test_incrby_parses_negative_increment() {
        let c = IncrBy::parse(&[bs("k"), bs("-3")]).unwrap();
        assert_eq!(c.increment, -3);
    }

    #[test]
    fn test_incrby_parses_zero() {
        let c = IncrBy::parse(&[bs("k"), bs("0")]).unwrap();
        assert_eq!(c.increment, 0);
    }

    #[test]
    fn test_incrby_with_too_few_args_is_error() {
        let r = IncrBy::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_incrby_with_too_many_args_is_error() {
        let r = IncrBy::parse(&[bs("k"), bs("1"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_incrby_with_non_integer_is_error() {
        let r = IncrBy::parse(&[bs("k"), bs("five")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_incrby_to_resp_args_round_trips() {
        let c = IncrBy::parse(&[bs("k"), bs("7")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[1], Bytes::from_static(b"7"));
    }

    #[test]
    fn test_incrby_to_resp_args_round_trips_negative() {
        let c = IncrBy::parse(&[bs("k"), bs("-2")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[1], Bytes::from_static(b"-2"));
    }
}
