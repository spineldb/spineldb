// src/core/commands/zset/zmscore.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::database::ExecutionContext;
use crate::core::database::zset::SortedSet;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZMScore {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ParseCommand for ZMScore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, members) = parse_key_and_values(args, 2, "ZMSCORE")?;
        Ok(ZMScore { key, members })
    }
}

#[async_trait]
impl ExecutableCommand for ZMScore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let zset_ref: Option<&SortedSet> = if let Some(entry) = shard_cache_guard.get_mut(&self.key)
        {
            if entry.is_expired() {
                None
            } else if let DataValue::SortedSet(zset) = &entry.data {
                Some(zset)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            None
        };

        let results: Vec<RespValue> = self
            .members
            .iter()
            .map(|member| {
                zset_ref
                    .and_then(|z| z.get_score(member))
                    .map(|s| RespValue::BulkString(s.to_string().into()))
                    .unwrap_or(RespValue::Null)
            })
            .collect();

        Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for ZMScore {
    fn name(&self) -> &'static str {
        "zmscore"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
        args.extend_from_slice(&self.members);
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
    fn test_zmscore_parses_key_and_members() {
        let c = ZMScore::parse(&[bs("z"), bs("m1"), bs("m2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"z"));
        assert_eq!(c.members.len(), 2);
    }

    #[test]
    fn test_zmscore_with_too_few_args_is_error() {
        let r = ZMScore::parse(&[bs("z")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_zmscore_with_non_bulk_key_is_wrong_type() {
        let r = ZMScore::parse(&[RespFrame::Integer(1), bs("m")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_zmscore_to_resp_args_round_trips() {
        let c = ZMScore::parse(&[bs("z"), bs("a"), bs("b")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
    }
}
