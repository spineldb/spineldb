// src/core/commands/string/mset.rs

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
pub struct MSet {
    pub pairs: Vec<RespFrame>,
}

impl ParseCommand for MSet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 || !args.len().is_multiple_of(2) {
            return Err(SpinelDBError::WrongArgumentCount("MSET".to_string()));
        }

        for arg in args {
            if !matches!(arg, RespFrame::BulkString(_)) {
                return Err(SpinelDBError::WrongType);
            }
        }

        Ok(MSet {
            pairs: args.to_vec(),
        })
    }
}

#[async_trait]
impl ExecutableCommand for MSet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if let ExecutionLocks::Multi { guards } = &mut ctx.locks {
            let keys_modified = (self.pairs.len() / 2) as u64;

            for pair_chunk in self.pairs.chunks_exact(2) {
                let key = extract_bytes(&pair_chunk[0])?;
                let value = extract_bytes(&pair_chunk[1])?;

                let shard_index = ctx.db.get_shard_index(&key);
                if let Some(guard) = guards.get_mut(&shard_index) {
                    let new_stored_value = StoredValue::new(DataValue::String(value));
                    guard.put(key, new_stored_value);
                }
            }
            Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Write { keys_modified },
            ))
        } else {
            Err(SpinelDBError::Internal(
                "MSET requires multi-shard lock".into(),
            ))
        }
    }
}

impl CommandSpec for MSet {
    fn name(&self) -> &'static str {
        "mset"
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
        self.pairs
            .iter()
            .step_by(2)
            .map(|frame| extract_bytes(frame).unwrap_or_default())
            .collect()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.pairs
            .iter()
            .map(|frame| extract_bytes(frame).unwrap_or_default())
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
    fn test_mset_parse_single_pair() {
        let c = MSet::parse(&[bs("k"), bs("v")]).unwrap();
        assert_eq!(c.pairs.len(), 2);
    }

    #[test]
    fn test_mset_parse_multiple_pairs() {
        let c = MSet::parse(&[bs("k1"), bs("v1"), bs("k2"), bs("v2")]).unwrap();
        assert_eq!(c.pairs.len(), 4);
    }

    #[test]
    fn test_mset_parse_empty_is_error() {
        let r = MSet::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_mset_parse_odd_args_is_error() {
        let r = MSet::parse(&[bs("k"), bs("v"), bs("k2")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_mset_parse_non_bulk_is_error() {
        let r = MSet::parse(&[bs("k"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_mset_to_resp_args() {
        let c = MSet {
            pairs: vec![bs("k1"), bs("v1"), bs("k2"), bs("v2")],
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[0], Bytes::from_static(b"k1"));
        assert_eq!(args[1], Bytes::from_static(b"v1"));
    }

    #[test]
    fn test_mset_spec() {
        let c = MSet { pairs: vec![] };
        assert_eq!(c.name(), "mset");
        assert_eq!(c.arity(), -3);
        assert!(c.flags().contains(CommandFlags::WRITE));
        assert_eq!(c.first_key(), 1);
        assert_eq!(c.step(), 2);
    }
}
