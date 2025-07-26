// src/core/commands/string/bitpos.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct BitPos {
    pub key: Bytes,
    pub bit: u8,
    pub range: Option<(i64, i64)>,
}

impl ParseCommand for BitPos {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 || args.len() > 4 {
            return Err(SpinelDBError::WrongArgumentCount("BITPOS".to_string()));
        }

        let bit = extract_string(&args[1])?
            .parse::<u8>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        if bit != 0 && bit != 1 {
            return Err(SpinelDBError::InvalidState(
                "bit is not an integer or out of range".to_string(),
            ));
        }

        let mut cmd = BitPos {
            key: extract_bytes(&args[0])?,
            bit,
            range: None,
        };

        if args.len() >= 3 {
            let start = extract_string(&args[2])?
                .parse::<i64>()
                .map_err(|_| SpinelDBError::NotAnInteger)?;

            let end = if args.len() == 4 {
                extract_string(&args[3])?
                    .parse::<i64>()
                    .map_err(|_| SpinelDBError::NotAnInteger)?
            } else {
                -1 // Default end is the last byte if only start is provided
            };
            cmd.range = Some((start, end));
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for BitPos {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        if let Some(entry) = shard_cache_guard.get(&self.key) {
            if entry.is_expired() {
                // Expired key behaves as if it doesn't exist.
            } else if let DataValue::String(s) = &entry.data {
                let len = s.len() as i64;
                let (start, end) = self.range.map_or((0, len - 1), |(s_start, s_end)| {
                    let start = if s_start < 0 { len + s_start } else { s_start };
                    let end = if s_end < 0 { len + s_end } else { s_end };
                    (start.max(0), end.min(len - 1))
                });

                if start > end {
                    return Ok((RespValue::Integer(-1), WriteOutcome::DidNotWrite));
                }

                for i in (start as usize)..=(end as usize) {
                    let byte = s[i];
                    // If we are looking for a '1', we test the byte directly.
                    // If we are looking for a '0', we test the inverted byte.
                    // If the result is not zero, it means the bit we are looking for is present.
                    let test_byte = if self.bit == 1 { byte } else { !byte };

                    if test_byte != 0 {
                        // trailing_zeros finds the position of the least significant '1' bit.
                        // For MSB-first RESP, we need to adjust from the right.
                        // Example: 0b00100000 -> trailing_zeros is 5. Bit pos is 7-5=2.
                        let bit_offset_from_right = test_byte.trailing_zeros();
                        let bit_offset_from_left = 7 - bit_offset_from_right;
                        return Ok((
                            RespValue::Integer((i as u32 * 8 + bit_offset_from_left) as i64),
                            WriteOutcome::DidNotWrite,
                        ));
                    }
                }

                // If no set bit was found and we're looking for a '0', the position is after the string.
                if self.bit == 0 {
                    return Ok((RespValue::Integer(len * 8), WriteOutcome::DidNotWrite));
                }
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }

        // Key does not exist or was expired.
        Ok((
            RespValue::Integer(if self.bit == 1 { -1 } else { 0 }),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl CommandSpec for BitPos {
    fn name(&self) -> &'static str {
        "bitpos"
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
        let mut args = vec![self.key.clone(), self.bit.to_string().into()];
        if let Some((start, end)) = self.range {
            args.push(start.to_string().into());
            args.push(end.to_string().into());
        }
        args
    }
}
