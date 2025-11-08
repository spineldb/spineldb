// src/core/commands/bloom/bf_reserve.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{BloomFilter, DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `BF.RESERVE` command, used to create a new Bloom filter.
///
/// This command allows pre-allocating a Bloom filter with a specified initial
/// capacity and desired error rate. If a Bloom filter already exists at the
/// given key, an error is returned.
#[derive(Debug, Clone, Default)]
pub struct BfReserve {
    /// The key under which the Bloom filter will be stored.
    pub key: Bytes,
    /// The desired error rate (false positive probability) for the Bloom filter.
    /// Must be between 0 and 1 (exclusive).
    pub error_rate: f64,
    /// The expected number of items to be added to the Bloom filter.
    /// Must be greater than 0.
    pub capacity: u64,
}

impl ParseCommand for BfReserve {
    /// Parses the arguments for the `BF.RESERVE` command.
    ///
    /// Expects three arguments: `key`, `error_rate`, and `capacity`.
    /// Validates `error_rate` to be (0, 1) and `capacity` to be > 0.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 3 {
            return Err(SpinelDBError::WrongArgumentCount("BF.RESERVE".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let error_rate = extract_string(&args[1])?
            .parse::<f64>()
            .map_err(|_| SpinelDBError::NotAFloat)?;
        let capacity = extract_string(&args[2])?
            .parse::<u64>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        if error_rate <= 0.0 || error_rate >= 1.0 {
            return Err(SpinelDBError::InvalidRequest(
                "error rate must be between 0 and 1".to_string(),
            ));
        }
        if capacity == 0 {
            return Err(SpinelDBError::InvalidRequest(
                "capacity must be greater than 0".to_string(),
            ));
        }

        Ok(BfReserve {
            key,
            error_rate,
            capacity,
        })
    }
}

#[async_trait]
impl ExecutableCommand for BfReserve {
    /// Executes the `BF.RESERVE` command.
    ///
    /// Creates a new Bloom filter with the specified parameters and stores it
    /// at the given key. Returns an error if the key already exists.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        if shard_cache_guard.peek(&self.key).is_some() {
            return Err(SpinelDBError::KeyExists);
        }

        let bf = BloomFilter::new(self.capacity, self.error_rate);
        let value = StoredValue::new(DataValue::BloomFilter(Box::new(bf)));
        shard_cache_guard.put(self.key.clone(), value);

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for BfReserve {
    /// Returns the name of the command, "bf.reserve".
    fn name(&self) -> &'static str {
        "bf.reserve"
    }
    /// Returns the arity of the command (command name + subcommand name + 3 arguments).
    fn arity(&self) -> i64 {
        4
    }
    /// Returns the flags for the `BF.RESERVE` command.
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
        vec![
            self.key.clone(),
            Bytes::from(self.error_rate.to_string()),
            Bytes::from(self.capacity.to_string()),
        ]
    }
}
