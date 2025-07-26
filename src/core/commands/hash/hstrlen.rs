// src/core/commands/hash/hstrlen.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct HStrLen {
    pub key: Bytes,
    pub field: Bytes,
}

impl ParseCommand for HStrLen {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "HSTRLEN")?;
        Ok(HStrLen {
            key: extract_bytes(&args[0])?,
            field: extract_bytes(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for HStrLen {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Menggunakan helper get_single_shard_context_mut untuk konsistensi dan keamanan.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Logika dirapikan dan semua nilai integer dibungkus dengan RespValue.
        let length = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                // Kunci ada tapi kedaluwarsa, sama seperti tidak ada.
                0
            } else if let DataValue::Hash(hash) = &entry.data {
                // Dapatkan panjang value dari field jika ada, jika tidak 0.
                hash.get(&self.field)
                    .map(|value| value.len() as i64)
                    .unwrap_or(0)
            } else {
                // Kunci ada tapi bukan tipe Hash.
                return Err(SpinelDBError::WrongType);
            }
        } else {
            // Kunci tidak ada sama sekali.
            0
        };

        Ok((RespValue::Integer(length), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for HStrLen {
    fn name(&self) -> &'static str {
        "hstrlen"
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
        vec![self.key.clone(), self.field.clone()]
    }
}
