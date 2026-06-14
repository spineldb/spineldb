// src/core/commands/vector/vs_reserve.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::vector::{DistanceMetric, SpinelVector};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.RESERVE` command to create a new vector index.
///
/// VS.RESERVE key dimension metric [CAPACITY capacity] [M m] [EF_CONSTRUCTION ef] [EF_SEARCH ef]
#[derive(Debug, Clone, Default)]
pub struct VsReserve {
    pub key: Bytes,
    pub dimension: u32,
    pub metric: DistanceMetric,
    pub capacity: u64,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

impl ParseCommand for VsReserve {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("VS.RESERVE".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let dimension = extract_string(&args[1])?
            .parse::<u32>()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        let metric_str = extract_string(&args[2])?;
        let metric = DistanceMetric::parse(&metric_str).ok_or_else(|| {
            SpinelDBError::InvalidRequest(format!(
                "invalid metric '{}', expected L2, COSINE, or IP",
                metric_str
            ))
        })?;

        let mut capacity: u64 = 10000;
        let mut m: usize = 16;
        let mut ef_construction: usize = 200;
        let mut ef_search: usize = 10;

        let mut i = 3;
        while i < args.len() {
            let opt = extract_string(&args[i])?.to_uppercase();
            match opt.as_str() {
                "CAPACITY" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.RESERVE".to_string()));
                    }
                    capacity = extract_string(&args[i])?
                        .parse::<u64>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                }
                "M" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.RESERVE".to_string()));
                    }
                    m = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                }
                "EF_CONSTRUCTION" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.RESERVE".to_string()));
                    }
                    ef_construction = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                }
                "EF_SEARCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.RESERVE".to_string()));
                    }
                    ef_search = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                }
                _ => {
                    return Err(SpinelDBError::InvalidRequest(format!(
                        "unknown option '{}'",
                        opt
                    )));
                }
            }
            i += 1;
        }

        if dimension == 0 {
            return Err(SpinelDBError::InvalidRequest(
                "dimension must be greater than 0".to_string(),
            ));
        }
        if capacity == 0 {
            return Err(SpinelDBError::InvalidRequest(
                "capacity must be greater than 0".to_string(),
            ));
        }
        if m < 2 {
            return Err(SpinelDBError::InvalidRequest(
                "M must be at least 2".to_string(),
            ));
        }
        if ef_construction < 1 {
            return Err(SpinelDBError::InvalidRequest(
                "EF_CONSTRUCTION must be at least 1".to_string(),
            ));
        }
        if ef_search < 1 {
            return Err(SpinelDBError::InvalidRequest(
                "EF_SEARCH must be at least 1".to_string(),
            ));
        }

        Ok(VsReserve {
            key,
            dimension,
            metric,
            capacity,
            m,
            ef_construction,
            ef_search,
        })
    }
}

#[async_trait]
impl ExecutableCommand for VsReserve {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        if shard_cache_guard.peek(&self.key).is_some() {
            return Err(SpinelDBError::KeyExists);
        }

        let sv = SpinelVector::new(
            self.dimension,
            self.metric,
            self.capacity,
            self.m,
            self.ef_construction,
            self.ef_search,
        );
        let value = StoredValue::new(DataValue::SpinelVector(Box::new(sv)));
        shard_cache_guard.put(self.key.clone(), value);

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for VsReserve {
    fn name(&self) -> &'static str {
        "vs.reserve"
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
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![
            self.key.clone(),
            Bytes::from(self.dimension.to_string()),
            Bytes::from(self.metric.as_str().to_string()),
        ];
        args.push(Bytes::from_static(b"CAPACITY"));
        args.push(Bytes::from(self.capacity.to_string()));
        args.push(Bytes::from_static(b"M"));
        args.push(Bytes::from(self.m.to_string()));
        args.push(Bytes::from_static(b"EF_CONSTRUCTION"));
        args.push(Bytes::from(self.ef_construction.to_string()));
        args.push(Bytes::from_static(b"EF_SEARCH"));
        args.push(Bytes::from(self.ef_search.to_string()));
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
    fn test_vsreserve_parse_valid() {
        let c = VsReserve::parse(&[bs("idx"), bs("128"), bs("COSINE")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.dimension, 128);
        assert_eq!(c.metric, DistanceMetric::Cosine);
        assert_eq!(c.capacity, 10000); // default
    }

    #[test]
    fn test_vsreserve_parse_with_options() {
        let c = VsReserve::parse(&[
            bs("idx"),
            bs("64"),
            bs("L2"),
            bs("CAPACITY"),
            bs("5000"),
            bs("M"),
            bs("32"),
            bs("EF_CONSTRUCTION"),
            bs("400"),
            bs("EF_SEARCH"),
            bs("20"),
        ])
        .unwrap();
        assert_eq!(c.capacity, 5000);
        assert_eq!(c.m, 32);
        assert_eq!(c.ef_construction, 400);
        assert_eq!(c.ef_search, 20);
    }

    #[test]
    fn test_vsreserve_too_few_args_is_error() {
        let r = VsReserve::parse(&[bs("idx"), bs("128")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsreserve_invalid_dimension_is_error() {
        let r = VsReserve::parse(&[bs("idx"), bs("0"), bs("L2")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_vsreserve_invalid_metric_is_error() {
        let r = VsReserve::parse(&[bs("idx"), bs("128"), bs("INVALID")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_vsreserve_to_resp_args_round_trip() {
        let c = VsReserve::parse(&[bs("idx"), bs("3"), bs("L2")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"idx"));
        assert_eq!(args[1], Bytes::from_static(b"3"));
        assert_eq!(args[2], Bytes::from_static(b"L2"));
    }
}
