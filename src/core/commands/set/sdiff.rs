// src/core/commands/set/sdiff.rs

use super::set_ops_logic::execute_sdiff;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Sdiff {
    pub keys: Vec<Bytes>,
}

impl ParseCommand for Sdiff {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SDIFF".to_string()));
        }
        let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(Sdiff { keys })
    }
}

#[async_trait]
impl ExecutableCommand for Sdiff {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Propagate WRONGTYPE error from execute_sdiff if any key is not a set.
        let diff_set = execute_sdiff(&self.keys, ctx).await?;

        let result = diff_set.into_iter().map(RespValue::BulkString).collect();
        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Sdiff {
    fn name(&self) -> &'static str {
        "sdiff"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_sdiff_parses_single_key() {
        let c = Sdiff::parse(&[bs("k1")]).unwrap();
        assert_eq!(c.keys.len(), 1);
        assert_eq!(c.keys[0], Bytes::from_static(b"k1"));
    }

    #[test]
    fn test_sdiff_parses_multiple_keys() {
        let c = Sdiff::parse(&[bs("k1"), bs("k2"), bs("k3")]).unwrap();
        assert_eq!(c.keys.len(), 3);
    }

    #[test]
    fn test_sdiff_with_no_args_is_error() {
        let r = Sdiff::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_sdiff_with_non_bulk_is_wrong_type() {
        let r = Sdiff::parse(&[bs("k1"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_sdiff_to_resp_args_round_trips() {
        let c = Sdiff::parse(&[bs("k1"), bs("k2")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args, vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]);
    }
}
