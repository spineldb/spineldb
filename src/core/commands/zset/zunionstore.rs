// src/core/commands/zset/zunionstore.rs

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

        let (weights, aggregate) = parse_store_args(&args[2 + num_keys..], num_keys)?;

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
        // Temporarily take ownership of the guards to pass to the helper.
        let mut temp_guards =
            match std::mem::replace(&mut ctx.locks, crate::core::database::ExecutionLocks::None) {
                crate::core::database::ExecutionLocks::Multi { guards } => guards,
                _ => {
                    return Err(SpinelDBError::Internal(
                        "ZUNIONSTORE requires multi-key lock".into(),
                    ));
                }
            };

        for key in &self.keys {
            zsets.push(get_zset_from_guard(key, ctx.db, &mut temp_guards)?.unwrap_or_default());
        }

        let result_zset = ZSetOp::union(&zsets, &self.weights, self.aggregate);

        // Put the guards back into the context before calling the store helper.
        ctx.locks = crate::core::database::ExecutionLocks::Multi {
            guards: temp_guards,
        };

        ZSetOp::store_result(self.destination.clone(), result_zset, ctx)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_zunionstore_parses_dest_numkeys_keys() {
        let c = ZUnionStore::parse(&[bs("dst"), bs("2"), bs("k1"), bs("k2")]).unwrap();
        assert_eq!(c.destination, Bytes::from_static(b"dst"));
        assert_eq!(c.keys.len(), 2);
        // Default weights are 1.0 per key.
        assert_eq!(c.weights, vec![1.0, 1.0]);
    }

    #[test]
    fn test_zunionstore_parses_with_weights() {
        let c = ZUnionStore::parse(&[
            bs("dst"),
            bs("2"),
            bs("k1"),
            bs("k2"),
            bs("WEIGHTS"),
            bs("1.5"),
            bs("2.0"),
        ])
        .unwrap();
        assert_eq!(c.weights.len(), 2);
        assert!((c.weights[0] - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_zunionstore_parses_with_aggregate_min() {
        let c = ZUnionStore::parse(&[bs("dst"), bs("1"), bs("k1"), bs("AGGREGATE"), bs("MIN")])
            .unwrap();
        assert!(matches!(c.aggregate, Aggregate::Min));
    }

    #[test]
    fn test_zunionstore_parses_with_aggregate_max() {
        let c = ZUnionStore::parse(&[bs("dst"), bs("1"), bs("k1"), bs("AGGREGATE"), bs("MAX")])
            .unwrap();
        assert!(matches!(c.aggregate, Aggregate::Max));
    }

    #[test]
    fn test_zunionstore_with_too_few_args_is_error() {
        let r = ZUnionStore::parse(&[bs("dst")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_zunionstore_with_zero_numkeys_is_syntax_error() {
        let r = ZUnionStore::parse(&[bs("dst"), bs("0")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_zunionstore_with_fewer_keys_than_numkeys_is_syntax_error() {
        let r = ZUnionStore::parse(&[bs("dst"), bs("3"), bs("k1")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_zunionstore_with_non_integer_numkeys_is_error() {
        let r = ZUnionStore::parse(&[bs("dst"), bs("all"), bs("k1")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_zunionstore_to_resp_args_round_trips() {
        let c = ZUnionStore::parse(&[bs("dst"), bs("1"), bs("k1")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"dst"));
        assert_eq!(args[1], Bytes::from_static(b"1"));
        assert_eq!(args[2], Bytes::from_static(b"k1"));
    }

    #[test]
    fn test_zunionstore_to_resp_args_with_aggregate() {
        let c = ZUnionStore::parse(&[bs("dst"), bs("1"), bs("k1"), bs("AGGREGATE"), bs("MAX")])
            .unwrap();
        let args = c.to_resp_args();
        assert!(args.contains(&Bytes::from_static(b"AGGREGATE")));
        assert!(args.contains(&Bytes::from_static(b"MAX")));
    }
}
