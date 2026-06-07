// src/core/commands/list/linsert.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum InsertPosition {
    #[default]
    Before,
    After,
}

#[derive(Debug, Clone, Default)]
pub struct LInsert {
    pub key: Bytes,
    pub position: InsertPosition,
    pub pivot: Bytes,
    pub element: Bytes,
}
impl ParseCommand for LInsert {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 4, "LINSERT")?;
        let key = extract_bytes(&args[0])?;
        let pos_str = extract_string(&args[1])?.to_ascii_lowercase();
        let position = match pos_str.as_str() {
            "before" => InsertPosition::Before,
            "after" => InsertPosition::After,
            _ => return Err(SpinelDBError::SyntaxError),
        };
        Ok(LInsert {
            key,
            position,
            pivot: extract_bytes(&args[2])?,
            element: extract_bytes(&args[3])?,
        })
    }
}
#[async_trait]
impl ExecutableCommand for LInsert {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }
        if let DataValue::List(list) = &mut entry.data {
            if let Some(pos) = list.iter().position(|x| *x == self.pivot) {
                let insert_at = match self.position {
                    InsertPosition::Before => pos,
                    InsertPosition::After => pos + 1,
                };
                list.insert(insert_at, self.element.clone());
                let mem_added = self.element.len();
                entry.size += mem_added;
                entry.version = entry.version.wrapping_add(1);
                shard.current_memory.fetch_add(mem_added, Ordering::Relaxed);
                Ok((
                    RespValue::Integer(list.len() as i64),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(-1), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}
impl CommandSpec for LInsert {
    fn name(&self) -> &'static str {
        "linsert"
    }
    fn arity(&self) -> i64 {
        5
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
        vec![
            self.key.clone(),
            (match self.position {
                InsertPosition::Before => "BEFORE",
                InsertPosition::After => "AFTER",
            })
            .into(),
            self.pivot.clone(),
            self.element.clone(),
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
    fn test_linsert_parses_before() {
        let c = LInsert::parse(&[bs("k"), bs("BEFORE"), bs("p"), bs("e")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.position, InsertPosition::Before);
        assert_eq!(c.pivot, Bytes::from_static(b"p"));
        assert_eq!(c.element, Bytes::from_static(b"e"));
    }

    #[test]
    fn test_linsert_parses_after() {
        let c = LInsert::parse(&[bs("k"), bs("AFTER"), bs("p"), bs("e")]).unwrap();
        assert_eq!(c.position, InsertPosition::After);
    }

    #[test]
    fn test_linsert_case_insensitive() {
        let c = LInsert::parse(&[bs("k"), bs("before"), bs("p"), bs("e")]).unwrap();
        assert_eq!(c.position, InsertPosition::Before);
    }

    #[test]
    fn test_linsert_with_too_few_args_is_error() {
        let r = LInsert::parse(&[bs("k"), bs("BEFORE"), bs("p")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_linsert_with_too_many_args_is_error() {
        let r = LInsert::parse(&[bs("k"), bs("BEFORE"), bs("p"), bs("e"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_linsert_with_invalid_position_is_syntax_error() {
        let r = LInsert::parse(&[bs("k"), bs("INSIDE"), bs("p"), bs("e")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_linsert_to_resp_args_round_trips() {
        let c = LInsert::parse(&[bs("k"), bs("AFTER"), bs("p"), bs("e")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(
            args,
            vec![
                Bytes::from_static(b"k"),
                Bytes::from_static(b"AFTER"),
                Bytes::from_static(b"p"),
                Bytes::from_static(b"e"),
            ]
        );
    }
}
