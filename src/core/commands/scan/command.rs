// src/core/commands/scan/command.rs

use super::helpers::{
    decode_scan_cursor, encode_scan_cursor, format_scan_options_to_bytes, glob_match,
    parse_scan_args,
};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::protocol::RespFrame;
use crate::core::storage::db::{ExecutionContext, NUM_SHARDS};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `SCAN` command for incrementally iterating over the keyspace.
#[derive(Debug, Clone, Default)]
pub struct Scan {
    pub cursor: u64,
    pub pattern: Option<Bytes>,
    pub count: Option<usize>,
}

impl ParseCommand for Scan {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (cursor, pattern, count) = parse_scan_args(args, 1, "SCAN")?;
        Ok(Scan {
            cursor,
            pattern,
            count,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Scan {
    /// Executes the SCAN command.
    /// This implementation iterates through the database shards one at a time,
    /// acquiring a lock only on the current shard to avoid blocking the entire database.
    /// The cursor encodes both the current shard index and the position within that shard.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Decode the cursor to determine the starting shard and internal position.
        let (mut current_shard_idx, mut internal_cursor) = decode_scan_cursor(self.cursor);
        let count = self.count.unwrap_or(10).max(1);
        let mut result_keys = Vec::with_capacity(count);

        // Iterate through shards until enough keys are found or all shards are scanned.
        'outer: while current_shard_idx < NUM_SHARDS {
            // Lock only the current shard for the duration of the scan within it.
            let shard = ctx.db.get_shard(current_shard_idx);
            let guard = shard.entries.lock().await;

            // Collect keys to iterate over. This provides a consistent view of the shard for this scan cycle.
            let keys_in_shard: Vec<Bytes> = guard.iter().map(|(k, _)| k.clone()).collect();
            let starting_point = internal_cursor;

            // Reset the internal cursor as we are moving to a new shard in the next iteration.
            internal_cursor = 0;

            for (i, key) in keys_in_shard.iter().enumerate().skip(starting_point) {
                // Verify that the key is not expired before including it in the results.
                if let Some(value) = guard.peek(key) {
                    if value.is_expired() {
                        continue;
                    }

                    // Apply pattern matching if a pattern is provided.
                    if let Some(p) = &self.pattern {
                        if glob_match(p, key) {
                            result_keys.push(RespValue::BulkString(key.clone()));
                        }
                    } else {
                        result_keys.push(RespValue::BulkString(key.clone()));
                    }
                }

                // If the desired count of keys is reached, stop and record the current position.
                if result_keys.len() >= count {
                    internal_cursor = i + 1;
                    break 'outer;
                }
            }

            // Move to the next shard for the subsequent iteration.
            current_shard_idx += 1;
        }

        // Encode the new cursor for the client's next call.
        // A cursor of 0 indicates that the entire iteration is complete.
        let new_cursor = if current_shard_idx >= NUM_SHARDS {
            0
        } else {
            encode_scan_cursor(current_shard_idx, internal_cursor)
        };

        // Format and return the response as `[new_cursor, [key1, key2, ...]]`.
        let resp = RespValue::Array(vec![
            RespValue::BulkString(new_cursor.to_string().into()),
            RespValue::Array(result_keys),
        ]);

        Ok((resp, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Scan {
    fn name(&self) -> &'static str {
        "scan"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }
    fn first_key(&self) -> i64 {
        0
    }
    fn last_key(&self) -> i64 {
        0
    }
    fn step(&self) -> i64 {
        0
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.cursor.to_string().into()];
        args.extend(format_scan_options_to_bytes(&self.pattern, &self.count));
        args
    }
}
