// src/core/commands/list/lpop.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::commands::list::logic::list_pop_logic;
use crate::core::database::{ExecutionContext, PopDirection};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct LPop {
    pub key: Bytes,
}
impl ParseCommand for LPop {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "LPOP")?;
        Ok(LPop {
            key: extract_bytes(&args[0])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for LPop {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        list_pop_logic(ctx, &self.key, PopDirection::Left).await
    }
}
impl CommandSpec for LPop {
    fn name(&self) -> &'static str {
        "lpop"
    }
    fn arity(&self) -> i64 {
        2
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
    fn test_lpop_parses_key() {
        let c = LPop::parse(&[bs("k")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
    }

    #[test]
    fn test_lpop_with_no_args_is_error() {
        let r = LPop::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lpop_with_too_many_args_is_error() {
        let r = LPop::parse(&[bs("k"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lpop_with_non_bulk_is_wrong_type() {
        let r = LPop::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_lpop_to_resp_args_round_trips() {
        let c = LPop::parse(&[bs("k")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"k")]);
    }
}
