// src/core/commands/bloom/bf_add.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::bloom::BloomFilter;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `BF.ADD` command, used to add an item to a Bloom filter.
///
/// If the Bloom filter specified by the key does not exist, it is implicitly
/// created with default parameters (capacity 100, error rate 0.01).
#[derive(Debug, Clone, Default)]
pub struct BfAdd {
    /// The key of the Bloom filter to add the item to.
    pub key: Bytes,
    /// The item to add to the Bloom filter.
    pub item: Bytes,
}

impl ParseCommand for BfAdd {
    /// Parses the arguments for the `BF.ADD` command.
    ///
    /// Expects two arguments: `key` and `item`.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 2 {
            return Err(SpinelDBError::WrongArgumentCount("BF.ADD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let item = extract_bytes(&args[1])?;
        Ok(BfAdd { key, item })
    }
}

#[async_trait]
impl ExecutableCommand for BfAdd {
    /// Executes the `BF.ADD` command.
    ///
    /// Adds the specified item to the Bloom filter. If the Bloom filter does not
    /// exist, it is created implicitly with default parameters.
    /// Returns 1 if the item was added (or might have been added), 0 if it was
    /// already considered present.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            // Create with default error rate (0.01) and capacity (100) if not exists
            let bf = BloomFilter::new(100, 0.01);
            StoredValue::new(DataValue::BloomFilter(Box::new(bf)))
        });

        if let DataValue::BloomFilter(ref mut bf) = entry.data {
            if bf.add(&self.item) {
                entry.version = entry.version.wrapping_add(1);
                Ok((
                    RespValue::Integer(1),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for BfAdd {
    /// Returns the name of the command, "bf.add".
    fn name(&self) -> &'static str {
        "bf.add"
    }
    /// Returns the arity of the command (command name + subcommand name + 2 arguments).
    fn arity(&self) -> i64 {
        3
    }
    /// Returns the flags for the `BF.ADD` command.
    ///
    /// This command writes to the dataset, can deny if out of memory, and its keys are movable.
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }
    /// Returns the 1-based index of the first key argument.
    fn first_key(&self) -> i64 {
        1
    }
    /// Returns the 1-based index of the last key argument.
    fn last_key(&self) -> i64 {
        1
    }
    /// Returns the step count between key arguments.
    fn step(&self) -> i64 {
        1
    }
    /// Extracts the key from the command arguments.
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    /// Converts the command's arguments back into a vector of `Bytes`.
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.key.clone(), self.item.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(parts: &[&str]) -> Vec<RespFrame> {
        parts
            .iter()
            .map(|s| RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes())))
            .collect()
    }

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_bfadd_parse_valid() {
        let frames = make_frame(&["mykey", "item1"]);
        let cmd = BfAdd::parse(&frames).unwrap();
        assert_eq!(cmd.key, Bytes::copy_from_slice(b"mykey"));
        assert_eq!(cmd.item, Bytes::copy_from_slice(b"item1"));
    }

    #[test]
    fn test_bfadd_parse_invalid() {
        let frames = make_frame(&["mykey"]);
        assert!(BfAdd::parse(&frames).is_err());
    }

    #[test]
    fn test_bfadd_too_few_args_is_error() {
        let frames = make_frame(&["k"]);
        let r = BfAdd::parse(&frames);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfadd_too_many_args_is_error() {
        let frames = make_frame(&["k", "item", "extra"]);
        let r = BfAdd::parse(&frames);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfadd_no_args_is_error() {
        let r = BfAdd::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfadd_with_non_bulk_key_is_wrong_type() {
        let r = BfAdd::parse(&[RespFrame::Integer(1), bs("item")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_bfadd_to_resp_args_round_trips() {
        let frames = make_frame(&["mykey", "item1"]);
        let c = BfAdd::parse(&frames).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"mykey"));
        assert_eq!(args[1], Bytes::from_static(b"item1"));
    }
}
