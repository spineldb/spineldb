// src/core/commands/string/getset.rs

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

#[derive(Debug, Clone, Default)]
pub struct GetSet {
    pub key: Bytes,
    pub value: Bytes,
}

impl ParseCommand for GetSet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "GETSET")?;
        Ok(GetSet {
            key: extract_bytes(&args[0])?,
            value: extract_bytes(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for GetSet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let new_value = StoredValue::new(DataValue::String(self.value.clone()));

        // `put` di ShardCache kami sudah diatur untuk menangani update memori dan mengembalikan nilai lama.
        // Namun, kita perlu menangani kasus tipe yang salah dan kedaluwarsa.
        let old_value = if let Some(old_entry) = shard_cache_guard.peek(&self.key) {
            if old_entry.is_expired() {
                RespValue::Null
            } else if let DataValue::String(s) = &old_entry.data {
                RespValue::BulkString(s.clone())
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            RespValue::Null
        };

        shard_cache_guard.put(self.key.clone(), new_value);

        Ok((old_value, WriteOutcome::Write { keys_modified: 1 }))
    }
}

impl CommandSpec for GetSet {
    fn name(&self) -> &'static str {
        "getset"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![self.key.clone(), self.value.clone()]
    }
}
