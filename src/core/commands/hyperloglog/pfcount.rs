// src/core/commands/hyperloglog/pfcount.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::hll::HyperLogLog;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct PfCount {
    pub keys: Vec<Bytes>,
}

impl ParseCommand for PfCount {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("PFCOUNT".to_string()));
        }
        let mut keys = Vec::new();
        for arg in args {
            keys.push(extract_bytes(arg)?);
        }
        Ok(PfCount { keys })
    }
}

use crate::core::database::locking::ExecutionLocks;

#[async_trait]
impl ExecutableCommand for PfCount {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if self.keys.is_empty() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let mut hlls = Vec::new();
        match &ctx.locks {
            ExecutionLocks::Multi { guards } => {
                for key in &self.keys {
                    let shard_index = ctx.db.get_shard_index(key);
                    let guard = guards.get(&shard_index).ok_or_else(|| {
                        SpinelDBError::Internal("Shard lock not found for key".to_string())
                    })?;

                    if let Some(entry) = guard.peek(key) {
                        if let DataValue::HyperLogLog(hll) = &entry.data {
                            hlls.push(hll.as_ref().clone());
                        } else {
                            return Err(SpinelDBError::WrongType);
                        }
                    } else {
                        hlls.push(HyperLogLog::new());
                    }
                }
            }
            ExecutionLocks::Single { guard, .. } => {
                let key = &self.keys[0];
                if let Some(entry) = guard.peek(key) {
                    if let DataValue::HyperLogLog(hll) = &entry.data {
                        hlls.push(hll.as_ref().clone());
                    } else {
                        return Err(SpinelDBError::WrongType);
                    }
                } else {
                    hlls.push(HyperLogLog::new());
                }
            }
            _ => {
                return Err(SpinelDBError::LockingError(
                    "Expected single or multi-shard lock".into(),
                ));
            }
        }

        if self.keys.len() == 1 {
            let count = hlls[0].count();
            return Ok((RespValue::Integer(count as i64), WriteOutcome::DidNotWrite));
        }

        let mut merged_hll = hlls.remove(0);
        for hll in hlls {
            merged_hll.merge(&hll);
        }

        let count = merged_hll.count();
        Ok((RespValue::Integer(count as i64), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for PfCount {
    fn name(&self) -> &'static str {
        "pfcount"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1 // -1 means last argument
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
}
