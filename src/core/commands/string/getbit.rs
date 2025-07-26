// src/core/commands/string/getbit.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct GetBit {
    pub key: Bytes,
    pub offset: u64,
}

impl ParseCommand for GetBit {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "GETBIT")?;
        let offset = extract_string(&args[1])?
            .parse::<u64>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;
        Ok(GetBit {
            key: extract_bytes(&args[0])?,
            offset,
        })
    }
}

#[async_trait]
impl ExecutableCommand for GetBit {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Use consistent helper to get the shard guard.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let value = if let Some(entry) = shard_cache_guard.get(&self.key) {
            if entry.is_expired() {
                0 // Expired keys are treated as non-existent.
            } else if let DataValue::String(s) = &entry.data {
                let byte_index = (self.offset / 8) as usize;
                if byte_index >= s.len() {
                    // Offset is out of bounds, so the bit is 0.
                    0
                } else {
                    let bit_in_byte_offset = 7 - (self.offset % 8) as u8; // SpinelDB bit order is MSB to LSB
                    let mask = 1 << bit_in_byte_offset;
                    ((s[byte_index] & mask) >> bit_in_byte_offset) as i64
                }
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            // Key does not exist, so the bit is 0.
            0
        };
        Ok((RespValue::Integer(value), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for GetBit {
    fn name(&self) -> &'static str {
        "getbit"
    }

    fn arity(&self) -> i64 {
        3
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
        vec![self.key.clone(), self.offset.to_string().into()]
    }
}
