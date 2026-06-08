// src/core/commands/string/msetnx.rs
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct MSetNx {
    pub pairs: Vec<(Bytes, Bytes)>,
}
impl ParseCommand for MSetNx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 || !args.len().is_multiple_of(2) {
            return Err(SpinelDBError::WrongArgumentCount("MSETNX".to_string()));
        }
        let pairs = args
            .chunks_exact(2)
            .map(|chunk| -> Result<(Bytes, Bytes), SpinelDBError> {
                Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?))
            })
            .collect::<Result<_, _>>()?;
        Ok(MSetNx { pairs })
    }
}
#[async_trait]
impl ExecutableCommand for MSetNx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if let ExecutionLocks::Multi { guards } = &mut ctx.locks {
            for (key, _) in &self.pairs {
                let shard_index = ctx.db.get_shard_index(key);
                if let Some(guard) = guards.get(&shard_index)
                    && guard.peek(key).is_some_and(|e| !e.is_expired())
                {
                    return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
                }
            }

            let keys_modified = self.pairs.len() as u64;
            for (key, value) in &self.pairs {
                let shard_index = ctx.db.get_shard_index(key);
                if let Some(guard) = guards.get_mut(&shard_index) {
                    let new_stored_value = StoredValue::new(DataValue::String(value.clone()));
                    guard.put(key.clone(), new_stored_value);
                }
            }
            return Ok((RespValue::Integer(1), WriteOutcome::Write { keys_modified }));
        }
        Err(SpinelDBError::Internal(
            "MSETNX requires multi-shard lock".into(),
        ))
    }
}
impl CommandSpec for MSetNx {
    fn name(&self) -> &'static str {
        "msetnx"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        2
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.pairs.iter().map(|(k, _)| k.clone()).collect()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.pairs
            .iter()
            .flat_map(|(k, v)| [k.clone(), v.clone()])
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_msetnx_parse_single_pair() {
        let c = MSetNx::parse(&[bs("k"), bs("v")]).unwrap();
        assert_eq!(c.pairs.len(), 1);
        assert_eq!(c.pairs[0].0, Bytes::from_static(b"k"));
        assert_eq!(c.pairs[0].1, Bytes::from_static(b"v"));
    }

    #[test]
    fn test_msetnx_parse_multiple_pairs() {
        let c = MSetNx::parse(&[bs("k1"), bs("v1"), bs("k2"), bs("v2")]).unwrap();
        assert_eq!(c.pairs.len(), 2);
    }

    #[test]
    fn test_msetnx_parse_empty_is_error() {
        let r = MSetNx::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_msetnx_parse_odd_args_is_error() {
        let r = MSetNx::parse(&[bs("k"), bs("v"), bs("k2")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_msetnx_parse_non_bulk_is_error() {
        let r = MSetNx::parse(&[bs("k"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_msetnx_to_resp_args() {
        let c = MSetNx {
            pairs: vec![(Bytes::from_static(b"a"), Bytes::from_static(b"1"))],
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn test_msetnx_spec() {
        let c = MSetNx { pairs: vec![] };
        assert_eq!(c.name(), "msetnx");
        assert_eq!(c.arity(), -3);
        assert!(c.flags().contains(CommandFlags::WRITE));
        assert_eq!(c.first_key(), 1);
        assert_eq!(c.step(), 2);
    }
}
