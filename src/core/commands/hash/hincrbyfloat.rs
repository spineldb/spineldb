// src/core/commands/hash/hincrbyfloat.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use indexmap::IndexMap;
use std::str;

/// Represents the `HINCRBYFLOAT` command.
#[derive(Debug, Clone, Default)]
pub struct HIncrByFloat {
    pub key: Bytes,
    pub field: Bytes,
    pub increment: f64,
}

impl ParseCommand for HIncrByFloat {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "HINCRBYFLOAT")?;
        Ok(HIncrByFloat {
            key: extract_bytes(&args[0])?,
            field: extract_bytes(&args[1])?,
            increment: extract_string(&args[2])?
                .parse()
                .map_err(|_| SpinelDBError::NotAFloat)?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for HIncrByFloat {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Get the hash or create a new one if the key does not exist.
        let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::Hash(IndexMap::new()))
        });

        // Scope the mutable borrow of `entry.data`.
        if let DataValue::Hash(hash) = &mut entry.data {
            let is_new_field = !hash.contains_key(&self.field);
            // Get the field's value or insert "0" if it's new.
            let field_value = hash.entry(self.field.clone()).or_insert_with(|| "0".into());

            let current_val: f64 = str::from_utf8(field_value)?
                .parse()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            let new_val = current_val + self.increment;

            let old_size = field_value.len();

            // Use ryu for efficient float-to-string formatting.
            let mut buffer = ryu::Buffer::new();
            let formatted_new_val = buffer.format(new_val);
            *field_value = Bytes::copy_from_slice(formatted_new_val.as_bytes());

            let new_size = field_value.len();

            // Calculate the memory difference.
            let mut mem_diff = new_size as isize - old_size as isize;
            if is_new_field {
                mem_diff += self.field.len() as isize;
            }

            // Safely update the entry size using saturating arithmetic.
            if mem_diff > 0 {
                entry.size = entry.size.saturating_add(mem_diff as usize);
            } else {
                entry.size = entry.size.saturating_sub((-mem_diff) as usize);
            }

            shard.update_memory(mem_diff);
            entry.version = entry.version.wrapping_add(1);

            Ok((
                // Return the newly formatted value.
                RespValue::BulkString(Bytes::copy_from_slice(formatted_new_val.as_bytes())),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for HIncrByFloat {
    fn name(&self) -> &'static str {
        "hincrbyfloat"
    }

    fn arity(&self) -> i64 {
        4
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
        let mut buffer = ryu::Buffer::new();
        let formatted_incr = buffer.format(self.increment);
        vec![
            self.key.clone(),
            self.field.clone(),
            Bytes::copy_from_slice(formatted_incr.as_bytes()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_hincrbyfloat_parses_positive_increment() {
        let c = HIncrByFloat::parse(&[bs("h"), bs("f"), bs("1.5")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"h"));
        assert_eq!(c.field, Bytes::from_static(b"f"));
        assert!((c.increment - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hincrbyfloat_parses_negative_increment() {
        let c = HIncrByFloat::parse(&[bs("h"), bs("f"), bs("-2.5")]).unwrap();
        assert!((c.increment - -2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hincrbyfloat_parses_integer_as_float() {
        let c = HIncrByFloat::parse(&[bs("h"), bs("f"), bs("10")]).unwrap();
        assert!((c.increment - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hincrbyfloat_with_too_few_args_is_error() {
        let r = HIncrByFloat::parse(&[bs("h"), bs("f")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_hincrbyfloat_with_too_many_args_is_error() {
        let r = HIncrByFloat::parse(&[bs("h"), bs("f"), bs("1.0"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_hincrbyfloat_with_non_float_increment_is_error() {
        let r = HIncrByFloat::parse(&[bs("h"), bs("f"), bs("abc")]);
        assert!(matches!(r, Err(SpinelDBError::NotAFloat)));
    }

    #[test]
    fn test_hincrbyfloat_to_resp_args_round_trips() {
        let c = HIncrByFloat::parse(&[bs("h"), bs("f"), bs("3.14")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"h"));
        assert_eq!(args[1], Bytes::from_static(b"f"));
    }
}
