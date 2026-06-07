// src/core/commands/list/lpush.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::commands::list::logic::list_push_logic;
use crate::core::database::{ExecutionContext, PushDirection};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct LPush {
    pub key: Bytes,
    pub values: Vec<Bytes>,
}

impl ParseCommand for LPush {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, values) = parse_key_and_values(args, 2, "LPUSH")?;
        Ok(LPush { key, values })
    }
}

#[async_trait]
impl ExecutableCommand for LPush {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        list_push_logic(ctx, &self.key, &self.values, PushDirection::Left).await
    }
}

impl CommandSpec for LPush {
    fn name(&self) -> &'static str {
        "lpush"
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
        let mut args = vec![self.key.clone()];
        args.extend(self.values.clone());
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
    fn test_lpush_parses_single_value() {
        let c = LPush::parse(&[bs("k"), bs("v1")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.values.len(), 1);
        assert_eq!(c.values[0], Bytes::from_static(b"v1"));
    }

    #[test]
    fn test_lpush_parses_multiple_values() {
        let c = LPush::parse(&[bs("k"), bs("a"), bs("b"), bs("c")]).unwrap();
        assert_eq!(c.values.len(), 3);
    }

    #[test]
    fn test_lpush_with_too_few_args_is_error() {
        let r = LPush::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lpush_with_non_bulk_key_is_wrong_type() {
        let r = LPush::parse(&[RespFrame::Integer(1), bs("v")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_lpush_with_non_bulk_value_is_wrong_type() {
        let r = LPush::parse(&[bs("k"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_lpush_to_resp_args_round_trips() {
        let c = LPush::parse(&[bs("k"), bs("a"), bs("b")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"a"));
        assert_eq!(args[2], Bytes::from_static(b"b"));
    }
}
