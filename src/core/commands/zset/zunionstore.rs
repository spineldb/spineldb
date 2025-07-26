// src/core/commands/zset/zunionstore.rs

use super::zset_ops_logic::{Aggregate, ZSetOp, get_zset_from_guard};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZUnionStore {
    pub destination: Bytes,
    pub keys: Vec<Bytes>,
    pub weights: Vec<f64>,
    pub aggregate: Aggregate,
}

impl ParseCommand for ZUnionStore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("ZUNIONSTORE".to_string()));
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

        let mut weights = vec![1.0; num_keys];
        let mut aggregate = Aggregate::Sum;
        let mut i = 2 + num_keys;

        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "weights" => {
                    i += 1;
                    if args.len() < i + num_keys {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    weights = args[i..i + num_keys]
                        .iter()
                        .map(|f| {
                            extract_string(f).and_then(|s| {
                                s.parse::<f64>().map_err(|_| SpinelDBError::NotAFloat)
                            })
                        })
                        .collect::<Result<_, _>>()?;
                    i += num_keys;
                }
                "aggregate" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    aggregate = match extract_string(&args[i])?.to_ascii_lowercase().as_str() {
                        "sum" => Aggregate::Sum,
                        "min" => Aggregate::Min,
                        "max" => Aggregate::Max,
                        _ => return Err(SpinelDBError::SyntaxError),
                    };
                    i += 1;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }

        Ok(ZUnionStore {
            destination,
            keys,
            weights,
            aggregate,
        })
    }
}

#[async_trait]
impl ExecutableCommand for ZUnionStore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut zsets = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            zsets.push(get_zset_from_guard(key, ctx).await?.unwrap_or_default());
        }

        let result_zset = ZSetOp::union(&zsets, &self.weights, self.aggregate);
        let len = result_zset.len();

        ZSetOp::store_result(self.destination.clone(), result_zset, ctx)?;

        Ok((
            RespValue::Integer(len as i64),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for ZUnionStore {
    fn name(&self) -> &'static str {
        "zunionstore"
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
        0 // Tidak bisa di-step karena numkeys
    }
    fn get_keys(&self) -> Vec<Bytes> {
        let mut all_keys = vec![self.destination.clone()];
        all_keys.extend_from_slice(&self.keys);
        all_keys
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.destination.clone(), self.keys.len().to_string().into()];
        args.extend_from_slice(&self.keys);

        // Periksa apakah bobotnya bukan default (semua 1.0)
        let is_weights_default = self.weights.iter().all(|&w| (w - 1.0).abs() < f64::EPSILON);
        if !is_weights_default {
            args.push("WEIGHTS".into());
            args.extend(self.weights.iter().map(|w| w.to_string().into()));
        }

        // Periksa apakah agregatnya bukan default (SUM)
        if !matches!(self.aggregate, Aggregate::Sum) {
            args.push("AGGREGATE".into());
            let agg_str = match self.aggregate {
                Aggregate::Sum => unreachable!(), // Sudah diperiksa di atas
                Aggregate::Min => "MIN",
                Aggregate::Max => "MAX",
            };
            args.push(agg_str.into());
        }
        args
    }
}
