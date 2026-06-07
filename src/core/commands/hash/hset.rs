// src/core/commands/hash/hset.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_field_value_pairs;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use indexmap::IndexMap;

/// Represents the `HSET` command.
#[derive(Debug, Clone, Default)]
pub struct HSet {
    pub key: Bytes,
    pub fields: Vec<(Bytes, Bytes)>,
}

impl ParseCommand for HSet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, fields) = parse_key_and_field_value_pairs(args, "HSET")?;
        Ok(HSet { key, fields })
    }
}

#[async_trait]
impl ExecutableCommand for HSet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if self.fields.is_empty() {
            // HSET with no field-value pairs is a syntax error in Redis.
            return Err(SpinelDBError::WrongArgumentCount("HSET".to_string()));
        }

        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Get the hash or create a new one if the key does not exist.
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::Hash(IndexMap::new()))
        });

        // Scope the mutable borrow of `entry.data`.
        let (new_fields_count, mem_diff) = {
            if let DataValue::Hash(hash) = &mut entry.data {
                let mut new_count = 0;
                let mut diff: isize = 0;

                for (field, value) in &self.fields {
                    let field_size = field.len();
                    let value_size = value.len();

                    // `insert` returns the old value if the key existed.
                    if let Some(old_value) = hash.insert(field.clone(), value.clone()) {
                        // Field was updated. Calculate the memory difference.
                        diff += value_size as isize - old_value.len() as isize;
                    } else {
                        // New field was added.
                        new_count += 1;
                        diff += (field_size + value_size) as isize;
                    }
                }
                (new_count, diff)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        };

        let outcome = if mem_diff != 0 || new_fields_count > 0 {
            // Re-borrow mutably after the inner scope is dropped.
            let entry = shard_cache_guard.get_mut(&self.key).unwrap();

            entry.version = entry.version.wrapping_add(1);
            // Safely update the entry size using saturating arithmetic.
            if mem_diff > 0 {
                entry.size = entry.size.saturating_add(mem_diff as usize);
            } else {
                entry.size = entry.size.saturating_sub((-mem_diff) as usize);
            }

            shard.update_memory(mem_diff);
            WriteOutcome::Write { keys_modified: 1 }
        } else {
            WriteOutcome::DidNotWrite
        };

        Ok((RespValue::Integer(new_fields_count), outcome))
    }
}

impl CommandSpec for HSet {
    fn name(&self) -> &'static str {
        "hset"
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
        let mut args = vec![self.key.clone()];
        for (k, v) in &self.fields {
            args.push(k.clone());
            args.push(v.clone());
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
    fn test_hset_parses_single_field() {
        let c = HSet::parse(&[bs("k"), bs("f1"), bs("v1")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.fields.len(), 1);
        assert_eq!(c.fields[0].0, Bytes::from_static(b"f1"));
        assert_eq!(c.fields[0].1, Bytes::from_static(b"v1"));
    }

    #[test]
    fn test_hset_parses_multiple_fields() {
        let c = HSet::parse(&[bs("k"), bs("f1"), bs("v1"), bs("f2"), bs("v2")]).unwrap();
        assert_eq!(c.fields.len(), 2);
        assert_eq!(c.fields[1].0, Bytes::from_static(b"f2"));
        assert_eq!(c.fields[1].1, Bytes::from_static(b"v2"));
    }

    #[test]
    fn test_hset_with_no_pairs_is_error() {
        let r = HSet::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_hset_with_odd_args_is_error() {
        let r = HSet::parse(&[bs("k"), bs("f1"), bs("v1"), bs("f2")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_hset_with_non_bulk_key_is_wrong_type() {
        let r = HSet::parse(&[RespFrame::Integer(1), bs("f"), bs("v")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_hset_to_resp_args_round_trips() {
        let c = HSet::parse(&[bs("k"), bs("a"), bs("1"), bs("b"), bs("2")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 5);
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"a"));
        assert_eq!(args[2], Bytes::from_static(b"1"));
        assert_eq!(args[3], Bytes::from_static(b"b"));
        assert_eq!(args[4], Bytes::from_static(b"2"));
    }
}
