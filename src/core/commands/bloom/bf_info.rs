// src/core/commands/bloom/bf_info.rs

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

/// Implements the `BF.INFO` command to get information about a Bloom filter.
#[derive(Debug, Clone, Default)]
pub struct BfInfo {
    /// The key of the Bloom filter.
    pub key: Bytes,
}

impl ParseCommand for BfInfo {
    /// Parses arguments for the `BF.INFO` command.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("BF.INFO".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        Ok(BfInfo { key })
    }
}

#[async_trait]
impl ExecutableCommand for BfInfo {
    /// Executes the `BF.INFO` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        match shard_cache_guard.peek(&self.key) {
            Some(entry) => {
                if let DataValue::BloomFilter(bf) = &entry.data {
                    let response = RespValue::Array(vec![
                        RespValue::SimpleString("Capacity".into()),
                        RespValue::Integer(bf.capacity as i64),
                        RespValue::SimpleString("Size".into()),
                        RespValue::Integer(bf.bits.len() as i64),
                        RespValue::SimpleString("Number of hash functions".into()),
                        RespValue::Integer(bf.num_hashes as i64),
                        RespValue::SimpleString("Number of items inserted".into()),
                        RespValue::Integer(bf.items_added as i64),
                    ]);
                    Ok((response, WriteOutcome::DidNotWrite))
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
            None => Err(SpinelDBError::KeyNotFound),
        }
    }
}

impl CommandSpec for BfInfo {
    fn name(&self) -> &'static str {
        "bf.info"
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
