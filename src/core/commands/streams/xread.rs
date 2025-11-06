// src/core/commands/streams/xread.rs

//! Implements the `XREAD` command for reading entries from one or more streams.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::stream::{StreamEntry, StreamId};
use crate::core::stream_blocking::StreamBlockerResult;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Bound;
use std::time::Duration;

/// Represents the starting ID for an XREAD command on a specific stream.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamIdSpec {
    /// An exact ID (e.g., "12345-0").
    Exact(StreamId),
    /// The last entry in the stream ('$').
    Last,
}

/// The parsed `XREAD` command with all its options.
#[derive(Debug, Clone, Default)]
pub struct XRead {
    pub streams: Vec<(Bytes, StreamIdSpec)>,
    pub count: Option<usize>,
    pub block_timeout: Option<Duration>,
}

impl ParseCommand for XRead {
    /// Parses the `XREAD` command's arguments from a slice of `RespFrame`.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let mut cmd = XRead::default();
        let mut i = 0;

        // Parse optional arguments like COUNT and BLOCK.
        while i < args.len() {
            let Ok(arg_str) = extract_string(&args[i]) else {
                // Not a string, must be the start of the 'STREAMS' keyword.
                break;
            };
            match arg_str.to_ascii_lowercase().as_str() {
                "count" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    cmd.count = Some(extract_string(&args[i])?.parse()?);
                    i += 1;
                }
                "block" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let ms: u64 = extract_string(&args[i])?.parse()?;
                    cmd.block_timeout = Some(Duration::from_millis(ms));
                    i += 1;
                }
                "streams" => {
                    i += 1;
                    break;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }

        // Parse the mandatory 'STREAMS key... id...' part.
        let remaining_args = &args[i..];
        let num_args = remaining_args.len();
        if num_args == 0 || !num_args.is_multiple_of(2) {
            return Err(SpinelDBError::WrongArgumentCount("XREAD".to_string()));
        }

        let num_streams = num_args / 2;
        let keys = &remaining_args[0..num_streams];
        let ids = &remaining_args[num_streams..];

        for (key_frame, id_frame) in keys.iter().zip(ids.iter()) {
            let key = extract_bytes(key_frame)?;
            let id_str = extract_string(id_frame)?;
            let id_spec = if id_str == "$" {
                StreamIdSpec::Last
            } else {
                StreamIdSpec::Exact(
                    id_str
                        .parse::<StreamId>()
                        .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?,
                )
            };
            cmd.streams.push((key, id_spec));
        }

        if cmd.streams.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("XREAD".to_string()));
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for XRead {
    /// Executes the `XREAD` command, handling both blocking and non-blocking cases,
    /// and correctly managing cluster slot migrations while blocked.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // --- Phase 1: Initial Read Attempt ---
        let initial_results = self.read_from_streams(ctx).await?;

        // If data is found or if it's not a blocking command, return immediately.
        if !initial_results.is_empty() || self.block_timeout.is_none() {
            let response = if initial_results.is_empty() {
                RespValue::Null
            } else {
                Self::format_results_array(initial_results)
            };
            return Ok((response, WriteOutcome::DidNotWrite));
        }

        // --- Phase 2: Block if necessary ---
        let timeout = self.block_timeout.unwrap();
        let stream_keys: Vec<Bytes> = self.streams.iter().map(|(k, _)| k.clone()).collect();

        let state = ctx.state.clone();
        let block_result = state
            .stream_blocker_manager
            .block_on(ctx, &stream_keys, timeout)
            .await;

        // Handle the result of the blocking call.
        match block_result {
            StreamBlockerResult::TimedOut => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            StreamBlockerResult::Moved(slot) => {
                let addr = ctx
                    .state
                    .cluster
                    .as_ref()
                    .unwrap()
                    .get_node_for_slot(slot)
                    .map_or_else(String::new, |node| node.node_info.addr.clone());
                Err(SpinelDBError::Moved { slot, addr })
            }
            StreamBlockerResult::Woken => {
                // Woken up by a notification. The `block_on` function has already verified
                // that a meaningful change occurred. Proceed to re-read the data.
                let final_results = self.read_from_streams(ctx).await?;
                let response = if final_results.is_empty() {
                    RespValue::Null
                } else {
                    Self::format_results_array(final_results)
                };
                Ok((response, WriteOutcome::DidNotWrite))
            }
        }
    }
}

impl XRead {
    /// Formats the final result into the nested array structure expected by clients.
    fn format_results_array(results: Vec<(Bytes, Vec<StreamEntry>)>) -> RespValue {
        let response_array: Vec<RespValue> = results
            .into_iter()
            .map(|(stream_name, entries)| {
                let entry_values: Vec<RespValue> = entries
                    .into_iter()
                    .map(|entry| Self::format_entry(&entry.id, &entry))
                    .collect();
                RespValue::Array(vec![
                    RespValue::BulkString(stream_name),
                    RespValue::Array(entry_values),
                ])
            })
            .collect();
        RespValue::Array(response_array)
    }

    /// The core logic to read entries from streams based on their specified starting IDs.
    async fn read_from_streams<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<Vec<(Bytes, Vec<StreamEntry>)>, SpinelDBError> {
        let mut results = Vec::new();
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "XREAD requires multi-key lock".into(),
                ));
            }
        };

        // First, resolve all '$' IDs to their actual last-generated ID.
        let mut resolved_streams = HashMap::with_capacity(self.streams.len());
        for (key, id_spec) in &self.streams {
            let id = match id_spec {
                StreamIdSpec::Exact(id) => *id,
                StreamIdSpec::Last => {
                    let shard_index = ctx.db.get_shard_index(key);
                    guards
                        .get(&shard_index)
                        .and_then(|guard| guard.peek(key))
                        .and_then(|e| match &e.data {
                            DataValue::Stream(s) => Some(s.last_generated_id),
                            _ => None,
                        })
                        .unwrap_or_default()
                }
            };
            resolved_streams.insert(key.clone(), id);
        }

        // Now, iterate through the resolved streams and read the data.
        for (key, start_id) in &resolved_streams {
            let shard_index = ctx.db.get_shard_index(key);
            if let Some(guard) = guards.get(&shard_index)
                && let Some(entry) = guard.peek(key)
                && !entry.is_expired()
                && let DataValue::Stream(stream) = &entry.data
            {
                let range = stream
                    .entries
                    .range((Bound::Excluded(*start_id), Bound::Unbounded));

                let stream_results: Vec<StreamEntry> = range
                    .take(self.count.unwrap_or(usize::MAX))
                    .map(|(_, se)| se.clone())
                    .collect();

                if !stream_results.is_empty() {
                    results.push((key.clone(), stream_results));
                }
            }
        }
        Ok(results)
    }

    /// Formats a single stream entry into the `[id, [field, value, ...]]` array format.
    pub fn format_entry(id: &StreamId, entry: &StreamEntry) -> RespValue {
        let mut fields_array = Vec::with_capacity(entry.fields.len() * 2);
        for (k, v) in &entry.fields {
            fields_array.push(RespValue::BulkString(k.clone()));
            fields_array.push(RespValue::BulkString(v.clone()));
        }
        RespValue::Array(vec![
            RespValue::BulkString(id.to_string().into()),
            RespValue::Array(fields_array),
        ])
    }
}

impl CommandSpec for XRead {
    fn name(&self) -> &'static str {
        "xread"
    }

    fn arity(&self) -> i64 {
        -3
    }

    fn flags(&self) -> CommandFlags {
        let mut flags = CommandFlags::READONLY | CommandFlags::MOVABLEKEYS;
        if self.block_timeout.is_some() {
            flags.insert(CommandFlags::NO_PROPAGATE);
        }
        flags
    }

    fn first_key(&self) -> i64 {
        0 // Keys are parsed specially for XREAD.
    }

    fn last_key(&self) -> i64 {
        0
    }

    fn step(&self) -> i64 {
        0
    }

    fn get_keys(&self) -> Vec<Bytes> {
        self.streams.iter().map(|(k, _)| k.clone()).collect()
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = Vec::new();
        if let Some(count) = self.count {
            args.extend([Bytes::from_static(b"COUNT"), count.to_string().into()]);
        }
        if let Some(block) = self.block_timeout {
            args.extend([
                Bytes::from_static(b"BLOCK"),
                block.as_millis().to_string().into(),
            ]);
        }
        args.push(Bytes::from_static(b"STREAMS"));
        let (keys, ids): (Vec<_>, Vec<_>) = self.streams.iter().cloned().unzip();
        args.extend(keys);
        args.extend(ids.iter().map(|id_spec| match id_spec {
            StreamIdSpec::Exact(id) => id.to_string().into(),
            StreamIdSpec::Last => Bytes::from_static(b"$"),
        }));
        args
    }
}
