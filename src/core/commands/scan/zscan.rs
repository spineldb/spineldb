// src/core/commands/scan/zscan.rs

use super::helpers::{format_scan_options_to_bytes, glob_match, parse_scan_args};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `ZSCAN` command for incrementally iterating over members of a sorted set.
#[derive(Debug, Clone, Default)]
pub struct ZScan {
    pub key: Bytes,
    pub cursor: u64,
    pub pattern: Option<Bytes>,
    pub count: Option<usize>,
}
impl ParseCommand for ZScan {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("ZSCAN".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let (cursor, pattern, count) = parse_scan_args(&args[1..], 1, "ZSCAN")?;
        Ok(ZScan {
            key,
            cursor,
            pattern,
            count,
        })
    }
}
#[async_trait]
impl ExecutableCommand for ZScan {
    /// Executes the ZSCAN command.
    /// The cursor represents the starting index for iteration within the sorted set's internal storage.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let (cursor, items) = if let Some(entry) = guard.get_mut(&self.key) {
            if entry.is_expired() {
                // Return an empty result if the key is expired.
                (0, vec![])
            } else if let DataValue::SortedSet(zset) = &entry.data {
                let count = self.count.unwrap_or(10).max(1);
                let mut result_items = Vec::with_capacity(count * 2);
                let mut new_cursor_pos = self.cursor as usize;

                // Iterate from the stored cursor position.
                for entry in zset.iter().skip(self.cursor as usize) {
                    new_cursor_pos += 1;

                    // Apply pattern matching if specified.
                    if let Some(p) = &self.pattern {
                        if glob_match(p, &entry.member) {
                            result_items.push(RespValue::BulkString(entry.member.clone()));
                            result_items
                                .push(RespValue::BulkString(entry.score.to_string().into()));
                        }
                    } else {
                        result_items.push(RespValue::BulkString(entry.member.clone()));
                        result_items.push(RespValue::BulkString(entry.score.to_string().into()));
                    }

                    // Stop if the desired number of elements is found.
                    if result_items.len() / 2 >= count {
                        break;
                    }
                }

                // If the new cursor position is at or beyond the end, the next cursor is 0.
                let new_cursor = if new_cursor_pos >= zset.len() {
                    0
                } else {
                    new_cursor_pos as u64
                };

                (new_cursor, result_items)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            // Key does not exist, return cursor 0 and an empty array.
            (0, vec![])
        };

        // Format and return the response as `[new_cursor, [member1, score1, ...]]`.
        let resp = RespValue::Array(vec![
            RespValue::BulkString(cursor.to_string().into()),
            RespValue::Array(items),
        ]);

        Ok((resp, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for ZScan {
    fn name(&self) -> &'static str {
        "zscan"
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
        let mut args = vec![self.key.clone(), self.cursor.to_string().into()];
        args.extend(format_scan_options_to_bytes(&self.pattern, &self.count));
        args
    }
}
