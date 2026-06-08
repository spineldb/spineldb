// src/core/commands/string/decr.rs
use super::incr::do_incr_decr_by;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Decr {
    pub key: Bytes,
}
impl ParseCommand for Decr {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "DECR")?;
        Ok(Decr {
            key: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for Decr {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        do_incr_decr_by(&self.key, -1, ctx).await
    }
}
impl CommandSpec for Decr {
    fn name(&self) -> &'static str {
        "decr"
    }
    fn arity(&self) -> i64 {
        2
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
        vec![self.key.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_decr_parse_basic() {
        let c = Decr::parse(&[bs("mykey")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"mykey"));
    }

    #[test]
    fn test_decr_parse_empty_args_is_error() {
        let r = Decr::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_decr_parse_non_bulk_string_is_error() {
        let r = Decr::parse(&[RespFrame::Integer(123)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_decr_to_resp_args() {
        let c = Decr {
            key: Bytes::from_static(b"counter"),
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], Bytes::from_static(b"counter"));
    }

    #[test]
    fn test_decr_spec() {
        let c = Decr {
            key: Bytes::from_static(b"k"),
        };
        assert_eq!(c.name(), "decr");
        assert_eq!(c.arity(), 2);
        assert!(c.flags().contains(CommandFlags::WRITE));
        assert_eq!(c.first_key(), 1);
        assert_eq!(c.step(), 1);
    }
}
