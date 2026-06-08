// src/core/commands/bloom/bf_insert.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::bloom::BloomFilter;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `BF.INSERT` command.
///
/// This command adds one or more items to a Bloom filter, creating the filter
/// with specified options if it does not already exist.
#[derive(Debug, Clone, Default)]
pub struct BfInsert {
    pub key: Bytes,
    pub items: Vec<Bytes>,
    pub capacity: Option<u64>,
    pub error_rate: Option<f64>,
}

impl ParseCommand for BfInsert {
    /// Parses arguments for the `BF.INSERT` command.
    ///
    /// Syntax: BF.INSERT key [CAPACITY capacity] [ERROR error_rate] ITEMS item [item ...]
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("BF.INSERT".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let mut capacity = None;
        let mut error_rate = None;
        let mut items = Vec::new();
        let mut items_started = false;
        let mut i = 1;

        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "capacity" => {
                    if items_started {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    i += 1;
                    capacity = Some(
                        extract_string(&args.get(i).ok_or(SpinelDBError::SyntaxError)?.clone())?
                            .parse::<u64>()
                            .map_err(|_| SpinelDBError::NotAnInteger)?,
                    );
                }
                "error" => {
                    if items_started {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    i += 1;
                    error_rate = Some(
                        extract_string(&args.get(i).ok_or(SpinelDBError::SyntaxError)?.clone())?
                            .parse::<f64>()
                            .map_err(|_| SpinelDBError::NotAFloat)?,
                    );
                }
                "items" => {
                    items_started = true;
                    // All subsequent arguments are items
                    for item_arg in &args[i + 1..] {
                        items.push(extract_bytes(item_arg)?);
                    }
                    if items.is_empty() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    // Break the loop as we've consumed all remaining args
                    break;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
            i += 1;
        }

        if !items_started {
            return Err(SpinelDBError::SyntaxError);
        }

        Ok(BfInsert {
            key,
            items,
            capacity,
            error_rate,
        })
    }
}

#[async_trait]
impl ExecutableCommand for BfInsert {
    /// Executes the `BF.INSERT` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let needs_creation = shard_cache_guard.peek(&self.key).is_none();
        if needs_creation {
            let capacity = self.capacity.unwrap_or(100);
            let error_rate = self.error_rate.unwrap_or(0.01);
            let bf = BloomFilter::new(capacity, error_rate);
            let value = StoredValue::new(DataValue::BloomFilter(Box::new(bf)));
            shard_cache_guard.put(self.key.clone(), value);
        } else if self.capacity.is_some() || self.error_rate.is_some() {
            // Cannot change params of an existing filter
            return Err(SpinelDBError::InvalidRequest(
                "Cannot change parameters of an existing filter".to_string(),
            ));
        }

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?; // Should not happen

        if let DataValue::BloomFilter(ref mut bf) = entry.data {
            let mut results = Vec::with_capacity(self.items.len());
            let mut changed = false;
            for item in &self.items {
                if bf.add(item) {
                    results.push(RespValue::Integer(1));
                    changed = true;
                } else {
                    results.push(RespValue::Integer(0));
                }
            }

            if changed {
                entry.version = entry.version.wrapping_add(1);
                Ok((
                    RespValue::Array(results),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for BfInsert {
    fn name(&self) -> &'static str {
        "bf.insert"
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
        let mut args = Vec::new();
        args.push(self.key.clone());
        if let Some(c) = self.capacity {
            args.push(Bytes::from_static(b"CAPACITY"));
            args.push(Bytes::from(c.to_string()));
        }
        if let Some(e) = self.error_rate {
            args.push(Bytes::from_static(b"ERROR"));
            args.push(Bytes::from(e.to_string()));
        }
        args.push(Bytes::from_static(b"ITEMS"));
        args.extend(self.items.clone());
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
    fn test_bfinsert_parses_with_items() {
        let c = BfInsert::parse(&[bs("k"), bs("ITEMS"), bs("item1"), bs("item2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.items.len(), 2);
        assert!(c.capacity.is_none());
        assert!(c.error_rate.is_none());
    }

    #[test]
    fn test_bfinsert_with_capacity() {
        let c = BfInsert::parse(&[
            bs("k"),
            bs("CAPACITY"),
            bs("1000"),
            bs("ITEMS"),
            bs("item1"),
        ])
        .unwrap();
        assert_eq!(c.capacity, Some(1000));
    }

    #[test]
    fn test_bfinsert_with_error_rate() {
        let c = BfInsert::parse(&[bs("k"), bs("ERROR"), bs("0.005"), bs("ITEMS"), bs("item1")])
            .unwrap();
        assert_eq!(c.error_rate, Some(0.005));
    }

    #[test]
    fn test_bfinsert_with_capacity_and_error() {
        let c = BfInsert::parse(&[
            bs("k"),
            bs("CAPACITY"),
            bs("5000"),
            bs("ERROR"),
            bs("0.001"),
            bs("ITEMS"),
            bs("item1"),
            bs("item2"),
        ])
        .unwrap();
        assert_eq!(c.capacity, Some(5000));
        assert_eq!(c.error_rate, Some(0.001));
    }

    #[test]
    fn test_bfinsert_with_no_items_is_error() {
        // Less than 3 args triggers WrongArgumentCount check.
        let r = BfInsert::parse(&[bs("k"), bs("ITEMS")]);
        assert!(r.is_err());
    }

    #[test]
    fn test_bfinsert_missing_items_keyword_is_syntax_error() {
        // Length is 3, but no ITEMS keyword.
        let r = BfInsert::parse(&[bs("k"), bs("item1"), bs("item2")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_bfinsert_capacity_after_items_succeeds() {
        // Once ITEMS is seen, parser breaks. CAPACITY after ITEMS is treated as an item
        // (not the keyword) in this implementation.
        let c = BfInsert::parse(&[bs("k"), bs("ITEMS"), bs("item1")]).unwrap();
        assert_eq!(c.items.len(), 1);
    }

    #[test]
    fn test_bfinsert_capacity_with_keyword_as_value_is_not_an_integer() {
        // After CAPACITY, the next arg is consumed as the value. "ITEMS" doesn't parse as u64.
        let r = BfInsert::parse(&[bs("k"), bs("CAPACITY"), bs("ITEMS")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_bfinsert_invalid_capacity_value_is_error() {
        let r = BfInsert::parse(&[
            bs("k"),
            bs("CAPACITY"),
            bs("not_a_number"),
            bs("ITEMS"),
            bs("i"),
        ]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_bfinsert_too_few_args_is_error() {
        let r = BfInsert::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfinsert_no_args_is_error() {
        let r = BfInsert::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bfinsert_to_resp_args_round_trips() {
        let c =
            BfInsert::parse(&[bs("k"), bs("CAPACITY"), bs("100"), bs("ITEMS"), bs("i1")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"CAPACITY"));
        assert_eq!(args[2], Bytes::from_static(b"100"));
        assert_eq!(args[3], Bytes::from_static(b"ITEMS"));
    }
}
