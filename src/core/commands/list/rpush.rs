// src/core/commands/list/rpush.rs

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
pub struct RPush {
    pub key: Bytes,
    pub values: Vec<Bytes>,
}

impl ParseCommand for RPush {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, values) = parse_key_and_values(args, 2, "RPUSH")?;
        Ok(RPush { key, values })
    }
}

#[async_trait]
impl ExecutableCommand for RPush {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        list_push_logic(ctx, &self.key, &self.values, PushDirection::Right).await
    }
}

impl CommandSpec for RPush {
    fn name(&self) -> &'static str {
        "rpush"
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
    fn test_rpush_parses_single_value() {
        let c = RPush::parse(&[bs("k"), bs("v1")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.values.len(), 1);
    }

    #[test]
    fn test_rpush_parses_multiple_values() {
        let c = RPush::parse(&[bs("k"), bs("a"), bs("b"), bs("c")]).unwrap();
        assert_eq!(c.values.len(), 3);
    }

    #[test]
    fn test_rpush_with_too_few_args_is_error() {
        let r = RPush::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_rpush_with_non_bulk_key_is_wrong_type() {
        let r = RPush::parse(&[RespFrame::Integer(1), bs("v")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_rpush_to_resp_args_round_trips() {
        let c = RPush::parse(&[bs("k"), bs("a"), bs("b")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"k"));
    }
}
