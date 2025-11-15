// src/core/commands/bloom/bf_madd.rs

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

/// Implements the `BF.MADD` command, used to add multiple items to a Bloom filter.
///
/// If the Bloom filter specified by the key does not exist, it is implicitly
/// created with default parameters (capacity 100, error rate 0.01).
#[derive(Debug, Clone, Default)]
pub struct BfMAdd {
    /// The key of the Bloom filter.
    pub key: Bytes,
    /// The items to add to the Bloom filter.
    pub items: Vec<Bytes>,
}

impl ParseCommand for BfMAdd {
    /// Parses the arguments for the `BF.MADD` command.
    ///
    /// Expects at least two arguments: `key` and one or more `item`s.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("BF.MADD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let items = args[1..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<Bytes>, _>>()?;

        Ok(BfMAdd { key, items })
    }
}

#[async_trait]
impl ExecutableCommand for BfMAdd {
    /// Executes the `BF.MADD` command.
    ///
    /// Adds the specified items to the Bloom filter. If the Bloom filter does not
    /// exist, it is created implicitly with default parameters.
    /// Returns an array of integers, where each integer is 1 if the corresponding
    /// item was added, and 0 if it was already present.
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
            let mut results = Vec::with_capacity(self.items.len());
            let mut changed = false;
            for item in &self.items {
                if bf.add(item) {
                    results.push(RespValue::Integer(1));
                    changed = true;
                } else {
                    results.push(RespValue::Integer(0));
                }
            }

            if changed {
                entry.version = entry.version.wrapping_add(1);
                Ok((
                    RespValue::Array(results),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for BfMAdd {
    fn name(&self) -> &'static str {
        "bf.madd"
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
        let mut args = Vec::with_capacity(1 + self.items.len());
        args.push(self.key.clone());
        args.extend(self.items.clone());
        args
    }
}
