// src/core/commands/hyperloglog/pfadd.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct PfAdd {
    pub key: Bytes,
    pub elements: Vec<Bytes>,
}

impl ParseCommand for PfAdd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("PFADD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let mut elements = Vec::new();
        for arg in &args[1..] {
            elements.push(extract_bytes(arg)?);
        }
        Ok(PfAdd { key, elements })
    }
}

#[async_trait]
impl ExecutableCommand for PfAdd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if self.elements.is_empty() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::HyperLogLog(Box::default()))
        });

        if let DataValue::HyperLogLog(ref mut hll) = entry.data {
            let mut changed = false;
            for element in &self.elements {
                if hll.add(element) {
                    changed = true;
                }
            }

            if changed {
                entry.version = entry.version.wrapping_add(1);
                Ok((
                    RespValue::Integer(1),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for PfAdd {
    fn name(&self) -> &'static str {
        "pfadd"
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
        args.extend(self.elements.clone());
        args
    }
}
