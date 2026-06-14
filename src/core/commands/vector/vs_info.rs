// src/core/commands/vector/vs_info.rs

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

/// Implements the `VS.INFO` command to get information about a vector index.
///
/// VS.INFO key
#[derive(Debug, Clone, Default)]
pub struct VsInfo {
    pub key: Bytes,
}

impl ParseCommand for VsInfo {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("VS.INFO".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        Ok(VsInfo { key })
    }
}

#[async_trait]
impl ExecutableCommand for VsInfo {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref sv) = entry.data {
            let mut info = Vec::new();

            let fields = vec![
                ("dimension", RespValue::Integer(sv.dimension() as i64)),
                (
                    "metric",
                    RespValue::BulkString(Bytes::from(sv.metric().as_str().to_string())),
                ),
                ("capacity", RespValue::Integer(sv.max_capacity() as i64)),
                ("size", RespValue::Integer(sv.len() as i64)),
                ("m", RespValue::Integer(sv.m() as i64)),
                (
                    "ef_construction",
                    RespValue::Integer(sv.ef_construction() as i64),
                ),
                ("ef_search", RespValue::Integer(sv.ef_search() as i64)),
                (
                    "deleted_count",
                    RespValue::Integer(sv.deleted_count() as i64),
                ),
                (
                    "vectors_added",
                    RespValue::Integer(sv.vectors_added() as i64),
                ),
                (
                    "vectors_deleted",
                    RespValue::Integer(sv.vectors_deleted() as i64),
                ),
                (
                    "memory_usage_bytes",
                    RespValue::Integer(sv.memory_usage_bytes() as i64),
                ),
            ];

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

impl CommandSpec for VsInfo {
    fn name(&self) -> &'static str {
        "vs.info"
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
    fn test_vsinfo_parse_valid() {
        let c = VsInfo::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
    }

    #[test]
    fn test_vsinfo_too_few_args() {
        let r = VsInfo::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsinfo_too_many_args() {
        let r = VsInfo::parse(&[bs("idx"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsinfo_to_resp_args() {
        let c = VsInfo::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"idx")]);
    }
}
