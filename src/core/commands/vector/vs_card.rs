// src/core/commands/vector/vs_card.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::vector::MetadataFilter;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.CARD` command to get the cardinality (count) of a vector index.
///
/// VS.CARD key [FILTER filter]
#[derive(Debug, Clone, Default)]
pub struct VsCard {
    pub key: Bytes,
    pub filter: Option<String>,
}

impl ParseCommand for VsCard {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("VS.CARD".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let mut filter = None;
        let mut i = 1;

        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            if upper == "FILTER" {
                i += 1;
                if i >= args.len() {
                    return Err(SpinelDBError::WrongArgumentCount("VS.CARD".to_string()));
                }
                filter = Some(extract_string(&args[i])?);
                i += 1;
            } else {
                return Err(SpinelDBError::InvalidRequest(format!(
                    "unknown option '{}'",
                    arg_str
                )));
            }
        }

        Ok(VsCard { key, filter })
    }
}

#[async_trait]
impl ExecutableCommand for VsCard {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref sv) = entry.data {
            let count = if let Some(ref filter_expr) = self.filter {
                let parsed_filter = MetadataFilter::parse_expr(filter_expr)
                    .map_err(SpinelDBError::InvalidRequest)?;
                sv.count_with_filter(&parsed_filter)
            } else {
                sv.len()
            };

            Ok((RespValue::Integer(count as i64), WriteOutcome::DidNotWrite))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsCard {
    fn name(&self) -> &'static str {
        "vs.card"
    }
    fn arity(&self) -> i64 {
        -2
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
        let mut args = vec![self.key.clone()];
        if let Some(ref filter) = self.filter {
            args.push(Bytes::from_static(b"FILTER"));
            args.push(Bytes::from(filter.clone()));
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
    fn test_vscard_parse_valid() {
        let c = VsCard::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert!(c.filter.is_none());
    }

    #[test]
    fn test_vscard_parse_with_filter() {
        let c = VsCard::parse(&[bs("idx"), bs("FILTER"), bs("type=a")]).unwrap();
        assert_eq!(c.filter, Some("type=a".to_string()));
    }

    #[test]
    fn test_vscard_too_few_args() {
        let r = VsCard::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vscard_to_resp_args() {
        let c = VsCard::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"idx")]);
    }

    #[test]
    fn test_vscard_to_resp_args_with_filter() {
        let c = VsCard::parse(&[bs("idx"), bs("FILTER"), bs("type=a")]).unwrap();
        let args = c.to_resp_args();
        assert!(args.contains(&Bytes::from_static(b"FILTER")));
    }
}
