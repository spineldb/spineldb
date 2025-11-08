// src/core/commands/hyperloglog/pfmerge.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::hll::HyperLogLog;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct PfMerge {
    pub dest_key: Bytes,
    pub source_keys: Vec<Bytes>,
}

impl ParseCommand for PfMerge {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("PFMERGE".to_string()));
        }
        let dest_key = extract_bytes(&args[0])?;
        let mut source_keys = Vec::new();
        for arg in &args[1..] {
            source_keys.push(extract_bytes(arg)?);
        }
        Ok(PfMerge {
            dest_key,
            source_keys,
        })
    }
}

use crate::core::database::locking::ExecutionLocks;

#[async_trait]
impl ExecutableCommand for PfMerge {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::LockingError(
                    "Expected multi-shard lock".into(),
                ));
            }
        };

        if self.source_keys.is_empty() {
            let dest_shard_index = ctx.db.get_shard_index(&self.dest_key);
            let dest_guard = guards.get_mut(&dest_shard_index).ok_or_else(|| {
                SpinelDBError::Internal("Shard lock not found for key".to_string())
            })?;
            dest_guard.get_or_insert_with_mut(self.dest_key.clone(), || {
                StoredValue::new(DataValue::HyperLogLog(Box::default()))
            });
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        // Get all source HyperLogLogs
        let mut source_hlls = Vec::new();
        for key in &self.source_keys {
            let shard_index = ctx.db.get_shard_index(key);
            let guard = guards.get(&shard_index).ok_or_else(|| {
                SpinelDBError::Internal("Shard lock not found for key".to_string())
            })?;

            if let Some(entry) = guard.peek(key) {
                if let DataValue::HyperLogLog(hll) = &entry.data {
                    source_hlls.push(hll.as_ref().clone());
                } else {
                    return Err(SpinelDBError::WrongType);
                }
            } else {
                // If key doesn't exist, create an empty HyperLogLog
                source_hlls.push(HyperLogLog::new());
            }
        }

        // Create merged HyperLogLog
        let mut merged_hll = HyperLogLog::new();
        for hll in source_hlls {
            merged_hll.merge(&hll);
        }

        // Store the result in the destination key
        let dest_shard_index = ctx.db.get_shard_index(&self.dest_key);
        let dest_guard = guards
            .get_mut(&dest_shard_index)
            .ok_or_else(|| SpinelDBError::Internal("Shard lock not found for key".to_string()))?;
        dest_guard.put(
            self.dest_key.clone(),
            StoredValue::new(DataValue::HyperLogLog(Box::new(merged_hll))),
        );

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for PfMerge {
    fn name(&self) -> &'static str {
        "pfmerge"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1 // dest key
    }
    fn last_key(&self) -> i64 {
        -1 // all remaining are source keys
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        let mut keys = vec![self.dest_key.clone()];
        keys.extend(self.source_keys.clone());
        keys
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.dest_key.clone()];
        args.extend(self.source_keys.clone());
        args
    }
}
