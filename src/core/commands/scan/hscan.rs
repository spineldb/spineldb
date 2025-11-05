// src/core/commands/scan/hscan.rs

use super::helpers::{format_scan_options_to_bytes, glob_match, parse_scan_args};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `HSCAN` command for incrementally iterating over fields in a hash.
#[derive(Debug, Clone, Default)]
pub struct HScan {
    pub key: Bytes,
    pub cursor: u64,
    pub pattern: Option<Bytes>,
    pub count: Option<usize>,
}
impl ParseCommand for HScan {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("HSCAN".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let (cursor, pattern, count) = parse_scan_args(&args[1..], 1, "HSCAN")?;
        Ok(HScan {
            key,
            cursor,
            pattern,
            count,
        })
    }
}
#[async_trait]
impl ExecutableCommand for HScan {
    /// Executes the HSCAN command.
    /// The cursor represents the starting index for iteration within the hash's internal storage.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let (cursor, items) = if let Some(entry) = guard.get_mut(&self.key) {
            if entry.is_expired() {
                // Return an empty result if the key is expired.
                (0, vec![])
            } else if let DataValue::Hash(hash) = &entry.data {
                let count = self.count.unwrap_or(10).max(1);
                let mut result_kvs = Vec::with_capacity(count * 2);
                let mut new_cursor_pos = self.cursor as usize;

                // Iterate from the stored cursor position.
                for (field, value) in hash.iter().skip(self.cursor as usize) {
                    new_cursor_pos += 1;

                    // Apply pattern matching if specified.
                    if let Some(p) = &self.pattern {
                        if glob_match(p, field) {
                            result_kvs.push(RespValue::BulkString(field.clone()));
                            result_kvs.push(RespValue::BulkString(value.clone()));
                        }
                    } else {
                        result_kvs.push(RespValue::BulkString(field.clone()));
                        result_kvs.push(RespValue::BulkString(value.clone()));
                    }

                    // Stop if the desired number of elements is found.
                    if result_kvs.len() / 2 >= count {
                        break;
                    }
                }

                // If the new cursor position is at or beyond the end, the next cursor is 0.
                let new_cursor = if new_cursor_pos >= hash.len() {
                    0
                } else {
                    new_cursor_pos as u64
                };

                (new_cursor, result_kvs)
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            // Key does not exist, return cursor 0 and an empty array.
            (0, vec![])
        };

        // Format and return the response as `[new_cursor, [field1, value1, ...]]`.
        let resp = RespValue::Array(vec![
            RespValue::BulkString(cursor.to_string().into()),
            RespValue::Array(items),
        ]);

        Ok((resp, WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for HScan {
    fn name(&self) -> &'static str {
        "hscan"
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
