// src/core/commands/hash/hincrby.rs

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
use bytes::Bytes;
use indexmap::IndexMap;
use std::str;

/// Represents the `HINCRBY` command.
#[derive(Debug, Clone, Default)]
pub struct HIncrBy {
    pub key: Bytes,
    pub field: Bytes,
    pub increment: i64,
}

impl ParseCommand for HIncrBy {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "HINCRBY")?;
        Ok(HIncrBy {
            key: extract_bytes(&args[0])?,
            field: extract_bytes(&args[1])?,
            increment: extract_string(&args[2])?
                .parse()
                .map_err(|_| SpinelDBError::NotAnInteger)?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for HIncrBy {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Get the hash or create a new one if the key does not exist.
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::Hash(IndexMap::new()))
        });

        // Scope the mutable borrow of `entry.data`.
        let (new_val, mem_diff) = {
            if let DataValue::Hash(hash) = &mut entry.data {
                let is_new_field = !hash.contains_key(&self.field);
                // Get the field's value or insert "0" if it's new.
                let field_value = hash.entry(self.field.clone()).or_insert_with(|| "0".into());

                let current_val: i64 = str::from_utf8(field_value)?
                    .parse()
                    .map_err(|_| SpinelDBError::NotAnInteger)?;

                let new_val_calc = current_val
                    .checked_add(self.increment)
                    .ok_or(SpinelDBError::Overflow)?;

                let old_size = field_value.len();
                *field_value = Bytes::from(new_val_calc.to_string());
                let new_size = field_value.len();

                // Calculate the memory difference.
                let mut diff = new_size as isize - old_size as isize;
                if is_new_field {
                    diff += self.field.len() as isize;
                }
                (new_val_calc, diff)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        };

        // Update the entry's metadata.
        let entry = shard_cache_guard.get_mut(&self.key).unwrap();
        // Safely update the entry size using saturating arithmetic.
        if mem_diff > 0 {
            entry.size = entry.size.saturating_add(mem_diff as usize);
        } else {
            entry.size = entry.size.saturating_sub((-mem_diff) as usize);
        }
        entry.version = entry.version.wrapping_add(1);
        shard.update_memory(mem_diff);

        Ok((
            RespValue::Integer(new_val),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for HIncrBy {
    fn name(&self) -> &'static str {
        "hincrby"
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
            self.field.clone(),
            self.increment.to_string().into(),
        ]
    }
}
