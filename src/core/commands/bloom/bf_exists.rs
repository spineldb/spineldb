// src/core/commands/bloom/bf_exists.rs

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

/// Implements the `BF.EXISTS` command, used to check if an item might be in a Bloom filter.
///
/// Returns 1 if the item might exist in the Bloom filter, 0 if it definitely
/// does not exist. If the key does not exist, it is treated as if the item
/// does not exist in an empty filter.
#[derive(Debug, Clone, Default)]
pub struct BfExists {
    /// The key of the Bloom filter to check.
    pub key: Bytes,
    /// The item to check for existence in the Bloom filter.
    pub item: Bytes,
}

impl ParseCommand for BfExists {
    /// Parses the arguments for the `BF.EXISTS` command.
    ///
    /// Expects two arguments: `key` and `item`.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 2 {
            return Err(SpinelDBError::WrongArgumentCount("BF.EXISTS".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let item = extract_bytes(&args[1])?;
        Ok(BfExists { key, item })
    }
}

#[async_trait]
impl ExecutableCommand for BfExists {
    /// Executes the `BF.EXISTS` command.
    ///
    /// Checks if the specified item might be present in the Bloom filter.
    /// Returns 1 if potentially present, 0 if definitely not present.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        match shard_cache_guard.peek(&self.key) {
            Some(entry) => {
                if let DataValue::BloomFilter(bf) = &entry.data {
                    if bf.check(&self.item) {
                        Ok((RespValue::Integer(1), WriteOutcome::DidNotWrite))
                    } else {
                        Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
                    }
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
            None => {
                // If the key doesn't exist, the item also doesn't exist.
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        }
    }
}

impl CommandSpec for BfExists {
    /// Returns the name of the command, "bf.exists".
    fn name(&self) -> &'static str {
        "bf.exists"
    }
    /// Returns the arity of the command (command name + subcommand name + 2 arguments).
    fn arity(&self) -> i64 {
        3
    }
    /// Returns the flags for the `BF.EXISTS` command.
    ///
    /// This command only reads from the dataset and its keys are movable.
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
