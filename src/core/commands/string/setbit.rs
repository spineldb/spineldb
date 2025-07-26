// src/core/commands/string/setbit.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct SetBit {
    pub key: Bytes,
    pub offset: u64,
    pub value: u8,
}

impl ParseCommand for SetBit {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "SETBIT")?;
        let offset = extract_string(&args[1])?
            .parse::<u64>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        let value = extract_string(&args[2])?
            .parse::<u8>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        if value != 0 && value != 1 {
            return Err(SpinelDBError::InvalidState(
                "bit is not an integer or out of range".to_string(),
            ));
        }

        Ok(SetBit {
            key: extract_bytes(&args[0])?,
            offset,
            value,
        })
    }
}

#[async_trait]
impl ExecutableCommand for SetBit {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::String(Bytes::new()))
        });

        if let DataValue::String(s) = &mut entry.data {
            let byte_index = (self.offset / 8) as usize;
            let bit_offset = (self.offset % 8) as u8;
            let mask = 1 << bit_offset;

            let old_size = s.len();
            let mut bytes = BytesMut::from(s.as_ref());

            // Perbesar string jika offset berada di luar jangkauan
            if byte_index >= old_size {
                bytes.resize(byte_index + 1, 0);
            }

            let original_byte = bytes[byte_index];
            let original_bit = (original_byte & mask) >> bit_offset;

            if self.value == 1 {
                bytes[byte_index] |= mask;
            } else {
                bytes[byte_index] &= !mask;
            }

            *s = bytes.freeze();

            let new_size = s.len();
            if new_size > old_size {
                let mem_added = new_size - old_size;
                entry.size += mem_added;
                shard.current_memory.fetch_add(mem_added, Ordering::Relaxed);
            }
            entry.version = entry.version.wrapping_add(1);

            Ok((
                RespValue::Integer(original_bit as i64),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for SetBit {
    fn name(&self) -> &'static str {
        "setbit"
    }

    fn arity(&self) -> i64 {
        4
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
        vec![
            self.key.clone(),
            self.offset.to_string().into(),
            self.value.to_string().into(),
        ]
    }
}
