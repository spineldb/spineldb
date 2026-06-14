// src/core/commands/vector/vs_stats.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.STATS` command to get detailed performance statistics.
///
/// VS.STATS key
#[derive(Debug, Clone, Default)]
pub struct VsStats {
    pub key: Bytes,
}

impl ParseCommand for VsStats {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("VS.STATS".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        Ok(VsStats { key })
    }
}

#[async_trait]
impl ExecutableCommand for VsStats {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref sv) = entry.data {
            let stats = sv.hnsw_stats();
            let memory = sv.memory_usage_bytes();

            let fields = vec![
                // Basic info
                ("dimension", RespValue::Integer(sv.dimension() as i64)),
                (
                    "metric",
                    RespValue::BulkString(Bytes::from(sv.metric().as_str().to_string())),
                ),
                ("capacity", RespValue::Integer(sv.max_capacity() as i64)),
                ("size", RespValue::Integer(sv.len() as i64)),
                // HNSW params
                ("m", RespValue::Integer(sv.m() as i64)),
                (
                    "ef_construction",
                    RespValue::Integer(sv.ef_construction() as i64),
                ),
                ("ef_search", RespValue::Integer(sv.ef_search() as i64)),
                // HNSW stats
                ("hnsw_max_level", RespValue::Integer(stats.max_level as i64)),
                (
                    "hnsw_node_count",
                    RespValue::Integer(stats.node_count as i64),
                ),
                (
                    "hnsw_deleted_count",
                    RespValue::Integer(stats.deleted_count as i64),
                ),
                (
                    "hnsw_total_inserts",
                    RespValue::Integer(stats.total_inserts as i64),
                ),
                (
                    "hnsw_total_searches",
                    RespValue::Integer(stats.total_searches as i64),
                ),
                (
                    "hnsw_total_deletes",
                    RespValue::Integer(stats.total_deletes as i64),
                ),
                // Memory
                ("memory_usage_bytes", RespValue::Integer(memory as i64)),
            ];

            let mut info = Vec::new();
            for (field, value) in fields {
                info.push(RespValue::BulkString(Bytes::from(field.to_string())));
                info.push(value);
            }

            Ok((RespValue::Array(info), WriteOutcome::DidNotWrite))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsStats {
    fn name(&self) -> &'static str {
        "vs.stats"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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
        vec![self.key.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_vsstats_parse_valid() {
        let c = VsStats::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
    }

    #[test]
    fn test_vsstats_too_few_args() {
        let r = VsStats::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsstats_too_many_args() {
        let r = VsStats::parse(&[bs("idx"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsstats_to_resp_args() {
        let c = VsStats::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"idx")]);
    }
}
