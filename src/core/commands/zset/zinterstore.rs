// src/core/commands/zset/zinterstore.rs

use super::zset_ops_logic::{Aggregate, ZSetOp, get_zset_from_guard, parse_store_args};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZInterStore {
    pub destination: Bytes,
    pub keys: Vec<Bytes>,
    pub weights: Vec<f64>,
    pub aggregate: Aggregate,
}

impl ParseCommand for ZInterStore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("ZINTERSTORE".to_string()));
        }
        let destination = extract_bytes(&args[0])?;
        let num_keys: usize = extract_string(&args[1])?
            .parse()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        if num_keys == 0 {
            return Err(SpinelDBError::SyntaxError);
        }
        if args.len() < 2 + num_keys {
            return Err(SpinelDBError::SyntaxError);
        }
        let keys: Vec<Bytes> = args[2..2 + num_keys]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        let (weights, aggregate) = parse_store_args(&args[2 + num_keys..], num_keys)?;

        Ok(ZInterStore {
            destination,
            keys,
            weights,
            aggregate,
        })
    }
}

#[async_trait]
impl ExecutableCommand for ZInterStore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut zsets = Vec::with_capacity(self.keys.len());
        // Temporarily take ownership of the guards to pass to the helper.
        let mut temp_guards =
            match std::mem::replace(&mut ctx.locks, crate::core::database::ExecutionLocks::None) {
                crate::core::database::ExecutionLocks::Multi { guards } => guards,
                _ => {
                    return Err(SpinelDBError::Internal(
                        "ZINTERSTORE requires multi-key lock".into(),
                    ));
                }
            };

        for key in &self.keys {
            if let Some(zset) = get_zset_from_guard(key, ctx.db, &mut temp_guards)? {
                zsets.push(zset);
            } else {
                // If any key doesn't exist, the intersection is empty.
                zsets.clear();
                break;
            }
        }

        let result_zset = ZSetOp::intersection(&zsets, &self.weights, self.aggregate);

        // Put the guards back into the context before calling the store helper.
        ctx.locks = crate::core::database::ExecutionLocks::Multi {
            guards: temp_guards,
        };

        ZSetOp::store_result(self.destination.clone(), result_zset, ctx)
    }
}

impl CommandSpec for ZInterStore {
    fn name(&self) -> &'static str {
        "zinterstore"
    }
    fn arity(&self) -> i64 {
        -4
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
        0 // Cannot be stepped due to numkeys argument
    }
    fn get_keys(&self) -> Vec<Bytes> {
        let mut all_keys = vec![self.destination.clone()];
        all_keys.extend_from_slice(&self.keys);
        all_keys
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.destination.clone(), self.keys.len().to_string().into()];
        args.extend_from_slice(&self.keys);

        let is_weights_default = self.weights.iter().all(|&w| (w - 1.0).abs() < f64::EPSILON);
        if !is_weights_default {
            args.push("WEIGHTS".into());
            args.extend(self.weights.iter().map(|w| w.to_string().into()));
        }

        if !matches!(self.aggregate, Aggregate::Sum) {
            args.push("AGGREGATE".into());
            let agg_str = match self.aggregate {
                Aggregate::Sum => unreachable!(),
                Aggregate::Min => "MIN",
                Aggregate::Max => "MAX",
            };
            args.push(agg_str.into());
        }
        args
    }
}
