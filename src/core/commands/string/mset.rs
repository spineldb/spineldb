// src/core/commands/string/mset.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct MSet {
    pub pairs: Vec<RespFrame>,
}

impl ParseCommand for MSet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 || !args.len().is_multiple_of(2) {
            return Err(SpinelDBError::WrongArgumentCount("MSET".to_string()));
        }

        for arg in args {
            if !matches!(arg, RespFrame::BulkString(_)) {
                return Err(SpinelDBError::WrongType);
            }
        }

        Ok(MSet {
            pairs: args.to_vec(),
        })
    }
}

#[async_trait]
impl ExecutableCommand for MSet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if let ExecutionLocks::Multi { guards } = &mut ctx.locks {
            let keys_modified = (self.pairs.len() / 2) as u64;

            for pair_chunk in self.pairs.chunks_exact(2) {
                let key = extract_bytes(&pair_chunk[0])?;
                let value = extract_bytes(&pair_chunk[1])?;

                let shard_index = ctx.db.get_shard_index(&key);
                if let Some(guard) = guards.get_mut(&shard_index) {
                    let new_stored_value = StoredValue::new(DataValue::String(value));
                    guard.put(key, new_stored_value);
                }
            }
            Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Write { keys_modified },
            ))
        } else {
            Err(SpinelDBError::Internal(
                "MSET requires multi-shard lock".into(),
            ))
        }
    }
}

impl CommandSpec for MSet {
    fn name(&self) -> &'static str {
        "mset"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        2
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.pairs
            .iter()
            .step_by(2)
            .map(|frame| extract_bytes(frame).unwrap_or_default())
            .collect()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.pairs
            .iter()
            .map(|frame| extract_bytes(frame).unwrap_or_default())
            .collect()
    }
}
