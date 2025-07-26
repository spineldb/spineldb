// src/core/commands/string/bitfield.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use tracing::warn;

/// A hard limit on memory allocation for a single BITFIELD operation to prevent DoS.
const MAX_BITFIELD_ALLOCATION: usize = 512 * 1024 * 1024; // 512 MB

/// Represents a specific BITFIELD sub-operation (GET, SET, INCRBY, OVERFLOW).
#[derive(Debug, Clone)]
pub enum BitFieldOp {
    Get(BitType, usize),
    Set(BitType, usize, i64),
    IncrBy(BitType, usize, i64),
    Overflow(OverflowBehavior),
}

/// Defines the overflow handling strategy for INCRBY operations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OverflowBehavior {
    Wrap,
    Sat,
    Fail,
}

/// Represents the integer type for a BITFIELD operation (e.g., i8, u16).
#[derive(Debug, Clone, Copy)]
pub struct BitType {
    is_signed: bool,
    bits: u8,
}

impl BitType {
    /// Parses a string like "i8" or "u32" into a BitType struct.
    fn from_str(type_str: &str) -> Result<Self, SpinelDBError> {
        if type_str.is_empty() {
            return Err(SpinelDBError::SyntaxError);
        }

        let first_char = type_str.chars().next().unwrap();
        let is_signed = first_char == 'i';
        let is_unsigned = first_char == 'u';

        if !is_signed && !is_unsigned {
            return Err(SpinelDBError::InvalidState(
                "Invalid bitfield type".to_string(),
            ));
        }

        let bits_str = &type_str[1..];
        let bits: u8 = bits_str
            .parse()
            .map_err(|_| SpinelDBError::InvalidState("Invalid bitfield type".to_string()))?;

        // Validate bit size according to Redis rules.
        if bits == 0 || (is_signed && bits > 64) || (is_unsigned && bits > 64) {
            return Err(SpinelDBError::InvalidState(
                "Invalid bitfield type".to_string(),
            ));
        }
        if is_unsigned && bits > 63 {
            return Err(SpinelDBError::InvalidState(
                "Unsigned integers of 64 bits are not supported for BITFIELD".to_string(),
            ));
        }

        Ok(BitType { is_signed, bits })
    }
}

/// The main `BITFIELD` command struct.
#[derive(Debug, Clone, Default)]
pub struct BitField {
    key: Bytes,
    operations: Vec<BitFieldOp>,
}

impl ParseCommand for BitField {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("BITFIELD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let mut operations = Vec::new();
        let mut i = 1;
        while i < args.len() {
            let op_str = extract_string(&args[i])?.to_ascii_lowercase();
            match op_str.as_str() {
                "get" => {
                    if i + 2 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let bit_type = BitType::from_str(&extract_string(&args[i + 1])?)?;
                    let offset = extract_string(&args[i + 2])?.parse()?;
                    operations.push(BitFieldOp::Get(bit_type, offset));
                    i += 3;
                }
                "set" => {
                    if i + 3 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let bit_type = BitType::from_str(&extract_string(&args[i + 1])?)?;
                    let offset = extract_string(&args[i + 2])?.parse()?;
                    let value = extract_string(&args[i + 3])?.parse()?;
                    operations.push(BitFieldOp::Set(bit_type, offset, value));
                    i += 4;
                }
                "incrby" => {
                    if i + 3 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let bit_type = BitType::from_str(&extract_string(&args[i + 1])?)?;
                    let offset = extract_string(&args[i + 2])?.parse()?;
                    let increment = extract_string(&args[i + 3])?.parse()?;
                    operations.push(BitFieldOp::IncrBy(bit_type, offset, increment));
                    i += 4;
                }
                "overflow" => {
                    if i + 1 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let behavior = match extract_string(&args[i + 1])?.to_ascii_lowercase().as_str()
                    {
                        "wrap" => OverflowBehavior::Wrap,
                        "sat" => OverflowBehavior::Sat,
                        "fail" => OverflowBehavior::Fail,
                        _ => return Err(SpinelDBError::SyntaxError),
                    };
                    operations.push(BitFieldOp::Overflow(behavior));
                    i += 2;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }
        Ok(BitField { key, operations })
    }
}

#[async_trait]
impl ExecutableCommand for BitField {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // --- Pre-flight Check ---
        let mut max_required_len_bytes = 0;
        for op in &self.operations {
            if let Some((bt, offset)) = match op {
                BitFieldOp::Get(bt, offset) => Some((*bt, *offset)),
                BitFieldOp::Set(bt, offset, _) => Some((*bt, *offset)),
                BitFieldOp::IncrBy(bt, offset, _) => Some((*bt, *offset)),
                BitFieldOp::Overflow(_) => None,
            } {
                let required_bits = offset
                    .checked_add(bt.bits as usize)
                    .ok_or(SpinelDBError::Overflow)?;
                max_required_len_bytes = max_required_len_bytes.max(required_bits.div_ceil(8));
            }
        }

        if max_required_len_bytes > MAX_BITFIELD_ALLOCATION {
            return Err(SpinelDBError::InvalidState(
                "bitfield allocation exceeds maximum size".to_string(),
            ));
        }

        if let Some(maxmem) = ctx.state.config.lock().await.maxmemory {
            let shard_index = ctx.db.get_shard_index(&self.key);
            let shard = ctx.db.get_shard(shard_index);

            let old_len_result =
                tokio::time::timeout(Duration::from_millis(5), shard.entries.lock()).await;

            let old_len = if let Ok(guard) = old_len_result {
                guard.peek(&self.key).map_or(0, |e| e.size)
            } else {
                warn!(
                    "Could not acquire lock for BITFIELD pre-flight check on key '{}' within 5ms. Using safe estimation.",
                    String::from_utf8_lossy(&self.key)
                );
                0
            };

            let estimated_increase = max_required_len_bytes.saturating_sub(old_len);
            let total_memory: usize = ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();

            if total_memory.saturating_add(estimated_increase) > maxmem
                && !ctx.db.evict_one_key(&ctx.state).await
            {
                return Err(SpinelDBError::MaxMemoryReached);
            }
        }

        // --- Execution Phase ---
        let (shard, guard) = ctx.get_single_shard_context_mut()?;
        let entry = guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::String(Bytes::new()))
        });

        if let DataValue::String(s) = &mut entry.data {
            let old_size = s.len();
            let mut bytes = BytesMut::from(s.as_ref());
            let mut results = Vec::new();
            let mut overflow_behavior = OverflowBehavior::Wrap;
            let mut is_write = false;

            for op in &self.operations {
                let required_len_bytes = match op {
                    BitFieldOp::Overflow(_) => continue,
                    BitFieldOp::Get(bt, offset)
                    | BitFieldOp::Set(bt, offset, _)
                    | BitFieldOp::IncrBy(bt, offset, _) => {
                        let new_len_bits = offset
                            .checked_add(bt.bits as usize)
                            .ok_or(SpinelDBError::Overflow)?;
                        new_len_bits.div_ceil(8)
                    }
                };

                if required_len_bytes > bytes.len() {
                    bytes.resize(required_len_bytes, 0);
                }

                match op {
                    BitFieldOp::Overflow(b) => overflow_behavior = *b,
                    BitFieldOp::Get(bit_type, offset) => {
                        let val = read_bits(&bytes, *offset, *bit_type);
                        results.push(RespValue::Integer(val));
                    }
                    BitFieldOp::Set(bit_type, offset, value) => {
                        is_write = true;
                        let old_val = read_bits(&bytes, *offset, *bit_type);
                        write_bits(&mut bytes, *offset, *bit_type, *value);
                        results.push(RespValue::Integer(old_val));
                    }
                    BitFieldOp::IncrBy(bit_type, offset, increment) => {
                        let current_val = read_bits(&bytes, *offset, *bit_type);
                        let (new_val, overflowed) = match overflow_behavior {
                            OverflowBehavior::Wrap => (current_val.wrapping_add(*increment), false),
                            OverflowBehavior::Sat => {
                                let (min, max) = get_bounds(*bit_type);
                                let res = current_val as i128 + *increment as i128;
                                if res > max as i128 {
                                    (max, true)
                                } else if res < min as i128 {
                                    (min, true)
                                } else {
                                    (res as i64, false)
                                }
                            }
                            OverflowBehavior::Fail => {
                                let (min, max) = get_bounds(*bit_type);
                                let (val_after_wrap, did_wrap) =
                                    current_val.overflowing_add(*increment);
                                if did_wrap || val_after_wrap > max || val_after_wrap < min {
                                    (0, true)
                                } else {
                                    (val_after_wrap, false)
                                }
                            }
                        };

                        if overflowed && overflow_behavior == OverflowBehavior::Fail {
                            results.push(RespValue::Null);
                        } else {
                            is_write = true;
                            write_bits(&mut bytes, *offset, *bit_type, new_val);
                            results.push(RespValue::Integer(new_val));
                        }
                    }
                }
            }

            *s = bytes.freeze();
            let new_size = s.len();

            let outcome = if is_write {
                let mem_diff = new_size as isize - old_size as isize;
                if mem_diff != 0 {
                    entry.size = new_size;
                    shard.update_memory(mem_diff);
                }
                entry.version += 1;
                WriteOutcome::Write { keys_modified: 1 }
            } else {
                WriteOutcome::DidNotWrite
            };

            Ok((RespValue::Array(results), outcome))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

/// Reads an arbitrary-width integer from a byte slice at a specific bit offset.
fn read_bits(bytes: &[u8], offset: usize, bit_type: BitType) -> i64 {
    let mut val: u64 = 0;
    for i in 0..bit_type.bits {
        let bit_pos = offset + i as usize;
        let byte_index = bit_pos / 8;
        let bit_in_byte = 7 - (bit_pos % 8);
        if byte_index < bytes.len() && (bytes[byte_index] >> bit_in_byte) & 1 != 0 {
            val |= 1 << (bit_type.bits - 1 - i);
        }
    }

    if bit_type.is_signed {
        let sign_bit_pos = bit_type.bits - 1;
        if (val >> sign_bit_pos) & 1 != 0 {
            let mask = u64::MAX << bit_type.bits;
            return (val | mask) as i64;
        }
    }
    val as i64
}

/// Writes an arbitrary-width integer to a byte slice at a specific bit offset.
fn write_bits(bytes: &mut [u8], offset: usize, bit_type: BitType, value: i64) {
    let val = value as u64;
    for i in 0..bit_type.bits {
        let bit_pos = offset + i as usize;
        let byte_index = bit_pos / 8;
        let bit_in_byte = 7 - (bit_pos % 8);
        let bit = (val >> (bit_type.bits - 1 - i)) & 1;
        if bit == 1 {
            bytes[byte_index] |= 1 << bit_in_byte;
        } else {
            bytes[byte_index] &= !(1 << bit_in_byte);
        }
    }
}

/// Helper to get the min/max values for a given integer type, used for SAT/FAIL overflow.
fn get_bounds(bit_type: BitType) -> (i64, i64) {
    if bit_type.is_signed {
        let max = (1i64 << (bit_type.bits - 1)) - 1;
        let min = -(1i64 << (bit_type.bits - 1));
        (min, max)
    } else {
        let max_unsigned = (1u64 << bit_type.bits).saturating_sub(1);
        (0, max_unsigned as i64)
    }
}

impl CommandSpec for BitField {
    fn name(&self) -> &'static str {
        "bitfield"
    }
    fn arity(&self) -> i64 {
        -2
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
        let mut args = vec![self.key.clone()];
        for op in &self.operations {
            match op {
                BitFieldOp::Get(bit_type, offset) => {
                    let type_str = if bit_type.is_signed { "i" } else { "u" };
                    args.extend_from_slice(&[
                        "GET".into(),
                        format!("{}{}", type_str, bit_type.bits).into(),
                        offset.to_string().into(),
                    ]);
                }
                BitFieldOp::Set(bit_type, offset, value) => {
                    let type_str = if bit_type.is_signed { "i" } else { "u" };
                    args.extend_from_slice(&[
                        "SET".into(),
                        format!("{}{}", type_str, bit_type.bits).into(),
                        offset.to_string().into(),
                        value.to_string().into(),
                    ]);
                }
                BitFieldOp::IncrBy(bit_type, offset, increment) => {
                    let type_str = if bit_type.is_signed { "i" } else { "u" };
                    args.extend_from_slice(&[
                        "INCRBY".into(),
                        format!("{}{}", type_str, bit_type.bits).into(),
                        offset.to_string().into(),
                        increment.to_string().into(),
                    ]);
                }
                BitFieldOp::Overflow(behavior) => {
                    let behavior_str = match behavior {
                        OverflowBehavior::Wrap => "WRAP",
                        OverflowBehavior::Sat => "SAT",
                        OverflowBehavior::Fail => "FAIL",
                    };
                    args.extend_from_slice(&["OVERFLOW".into(), behavior_str.into()]);
                }
            }
        }
        args
    }
}
