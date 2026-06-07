// src/core/commands/bloom/bf_card.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `BF.CARD` command to get the number of items in a Bloom filter.
#[derive(Debug, Clone, Default)]
pub struct BfCard {
    /// The key of the Bloom filter.
    pub key: Bytes,
}

impl ParseCommand for BfCard {
    /// Parses arguments for the `BF.CARD` command.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("BF.CARD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        Ok(BfCard { key })
    }
}

#[async_trait]
impl ExecutableCommand for BfCard {
    /// Executes the `BF.CARD` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        match shard_cache_guard.peek(&self.key) {
            Some(entry) => {
                if let DataValue::BloomFilter(bf) = &entry.data {
                    Ok((
                        RespValue::Integer(bf.items_added as i64),
                        WriteOutcome::DidNotWrite,
                    ))
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
            None => {
                // If the key doesn't exist, cardinality is 0.
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        }
    }
}

impl CommandSpec for BfCard {
    fn name(&self) -> &'static str {
        "bf.card"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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
    fn test_bfcard_parses_key() {
        let c = BfCard::parse(&[bs("mykey")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"mykey"));
    }

    #[test]
    fn test_bfcard_with_too_few_args_is_error() {
        let r = BfCard::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfcard_with_too_many_args_is_error() {
        let r = BfCard::parse(&[bs("k"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfcard_with_non_bulk_key_is_wrong_type() {
        let r = BfCard::parse(&[RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_bfcard_to_resp_args_round_trips() {
        let c = BfCard::parse(&[bs("k")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args, vec![Bytes::from_static(b"k")]);
    }
}
