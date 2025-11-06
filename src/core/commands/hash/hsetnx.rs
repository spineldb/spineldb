// src/core/commands/hash/hsetnx.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use indexmap::IndexMap;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct HSetNx {
    pub key: Bytes,
    pub field: Bytes,
    pub value: Bytes,
}

impl ParseCommand for HSetNx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "HSETNX")?;
        Ok(HSetNx {
            key: extract_bytes(&args[0])?,
            field: extract_bytes(&args[1])?,
            value: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for HSetNx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::Hash(IndexMap::new()))
        });

        if let DataValue::Hash(hash) = &mut entry.data {
            if hash.contains_key(&self.field) {
                return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
            }

            let mem_added = self.field.len() + self.value.len();
            hash.insert(self.field.clone(), self.value.clone());

            entry.size += mem_added;
            entry.version = entry.version.wrapping_add(1);
            shard.current_memory.fetch_add(mem_added, Ordering::Relaxed);

            Ok((
                RespValue::Integer(1),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for HSetNx {
    fn name(&self) -> &'static str {
        "hsetnx"
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
        vec![self.key.clone(), self.field.clone(), self.value.clone()]
    }
}
