// src/core/commands/bloom/bf_mexists.rs

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

/// Implements the `BF.MEXISTS` command to check if multiple items might be in a Bloom filter.
#[derive(Debug, Clone, Default)]
pub struct BfMExists {
    /// The key of the Bloom filter to check.
    pub key: Bytes,
    /// The items to check for existence.
    pub items: Vec<Bytes>,
}

impl ParseCommand for BfMExists {
    /// Parses arguments for the `BF.MEXISTS` command.
    ///
    /// Expects at least two arguments: `key` and one or more `item`s.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("BF.MEXISTS".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let items = args[1..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<Bytes>, _>>()?;

        Ok(BfMExists { key, items })
    }
}

#[async_trait]
impl ExecutableCommand for BfMExists {
    /// Executes the `BF.MEXISTS` command.
    ///
    /// Checks if the specified items might be present in the Bloom filter.
    /// Returns an array of integers, where each integer is 1 if the corresponding
    /// item might exist, and 0 if it definitely does not.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        match shard_cache_guard.peek(&self.key) {
            Some(entry) => {
                if let DataValue::BloomFilter(bf) = &entry.data {
                    let results = self
                        .items
                        .iter()
                        .map(|item| {
                            if bf.check(item) {
                                RespValue::Integer(1)
                            } else {
                                RespValue::Integer(0)
                            }
                        })
                        .collect();
                    Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
            None => {
                // If the key doesn't exist, none of the items exist.
                let results = vec![RespValue::Integer(0); self.items.len()];
                Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
            }
        }
    }
}

impl CommandSpec for BfMExists {
    fn name(&self) -> &'static str {
        "bf.mexists"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
        let mut args = Vec::with_capacity(1 + self.items.len());
        args.push(self.key.clone());
        args.extend(self.items.clone());
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
    fn test_bfmexists_parses_key_and_items() {
        let c = BfMExists::parse(&[bs("k"), bs("i1"), bs("i2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.items.len(), 2);
    }

    #[test]
    fn test_bfmexists_with_too_few_args_is_error() {
        let r = BfMExists::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfmexists_with_no_args_is_error() {
        let r = BfMExists::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfmexists_with_non_bulk_key_is_wrong_type() {
        let r = BfMExists::parse(&[RespFrame::Integer(1), bs("item")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_bfmexists_to_resp_args_round_trips() {
        let c = BfMExists::parse(&[bs("k"), bs("i1"), bs("i2"), bs("i3")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[0], Bytes::from_static(b"k"));
    }
}
