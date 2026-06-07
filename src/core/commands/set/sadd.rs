// src/core/commands/set/sadd.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct Sadd {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ParseCommand for Sadd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, members) = parse_key_and_values(args, 2, "SADD")?;
        Ok(Sadd { key, members })
    }
}

#[async_trait]
impl ExecutableCommand for Sadd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if self.members.is_empty() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::Set(HashSet::new()))
        });

        if let DataValue::Set(set) = &mut entry.data {
            let mut new_members_count = 0;
            let mut mem_added = 0;

            for member in &self.members {
                if set.insert(member.clone()) {
                    new_members_count += 1;
                    mem_added += member.len();
                }
            }

            let outcome = if new_members_count > 0 {
                entry.version = entry.version.wrapping_add(1);
                entry.size += mem_added;
                shard.current_memory.fetch_add(mem_added, Ordering::Relaxed);
                WriteOutcome::Write { keys_modified: 1 }
            } else {
                WriteOutcome::DidNotWrite
            };

            Ok((RespValue::Integer(new_members_count as i64), outcome))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for Sadd {
    fn name(&self) -> &'static str {
        "sadd"
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
        args.extend(self.members.clone());
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
    fn test_sadd_parses_key_and_members() {
        let c = Sadd::parse(&[bs("k"), bs("m1"), bs("m2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.members.len(), 2);
        assert_eq!(c.members[0], Bytes::from_static(b"m1"));
    }

    #[test]
    fn test_sadd_with_too_few_args_is_error() {
        let r = Sadd::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_sadd_with_non_bulk_key_is_wrong_type() {
        let r = Sadd::parse(&[RespFrame::Integer(1), bs("m")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_sadd_with_non_bulk_member_is_wrong_type() {
        let r = Sadd::parse(&[bs("k"), RespFrame::Integer(1)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_sadd_to_resp_args_round_trips() {
        let c = Sadd::parse(&[bs("k"), bs("a"), bs("b")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"k"));
    }
}
