// src/core/commands/streams/xreadgroup.rs

//! Implements the `XREADGROUP` command for reading from a stream as part of a consumer group.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::commands::streams::xread::XRead;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::storage::stream::{Consumer, PendingEntryInfo, StreamId};
use crate::core::stream_blocking::StreamBlockerResult;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Bound;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Represents the starting ID for an XREADGROUP command on a specific stream.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupStreamIdSpec {
    /// An exact ID (e.g., "0-0" for pending messages).
    Exact(StreamId),
    /// Read new entries not yet delivered to any consumer ('>').
    New,
}

/// The parsed `XREADGROUP` command with all its options.
#[derive(Debug, Clone, Default)]
pub struct XReadGroup {
    pub group_name: Bytes,
    pub consumer_name: Bytes,
    pub streams: Vec<(Bytes, GroupStreamIdSpec)>,
    pub count: Option<usize>,
    pub block_timeout: Option<Duration>,
    pub noack: bool,
}

impl ParseCommand for XReadGroup {
    /// Parses the `XREADGROUP` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let mut cmd = XReadGroup::default();
        let mut i = 0;

        // The command must start with 'GROUP <group_name> <consumer_name>'
        if args.len() < 3 || !extract_string(&args[i])?.eq_ignore_ascii_case("group") {
            return Err(SpinelDBError::SyntaxError);
        }
        i += 1;
        cmd.group_name = extract_bytes(&args[i])?;
        i += 1;
        cmd.consumer_name = extract_bytes(&args[i])?;
        i += 1;

        // Parse optional arguments
        while i < args.len() {
            let Ok(arg_str) = extract_string(&args[i]) else {
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
                "noack" => {
                    cmd.noack = true;
                    i += 1;
                }
                "streams" => {
                    i += 1;
                    break;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }

        // Parse the mandatory 'STREAMS key... id...' part
        let remaining_args = &args[i..];
        let num_args = remaining_args.len();
        if num_args == 0 || num_args % 2 != 0 {
            return Err(SpinelDBError::WrongArgumentCount("XREADGROUP".to_string()));
        }

        let num_streams = num_args / 2;
        let keys = &remaining_args[0..num_streams];
        let ids = &remaining_args[num_streams..];

        for (key_frame, id_frame) in keys.iter().zip(ids.iter()) {
            let key = extract_bytes(key_frame)?;
            let id_str = extract_string(id_frame)?;
            let id_spec = if id_str == ">" {
                GroupStreamIdSpec::New
            } else {
                GroupStreamIdSpec::Exact(
                    id_str
                        .parse::<StreamId>()
                        .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?,
                )
            };
            cmd.streams.push((key, id_spec));
        }

        if cmd.streams.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("XREADGROUP".to_string()));
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for XReadGroup {
    /// Executes the `XREADGROUP` command, handling reading from the PEL, reading new
    /// entries, and managing blocking state with cluster awareness.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // --- Phase 1: Initial Read Attempt ---
        let (initial_results, write_occurred) = self.read_from_streams(ctx).await?;
        if !initial_results.is_empty() || self.block_timeout.is_none() {
            let outcome = if write_occurred {
                WriteOutcome::Write { keys_modified: 1 } // Approximation for simplicity
            } else {
                WriteOutcome::DidNotWrite
            };
            let response = if initial_results.is_empty() {
                RespValue::Null
            } else {
                Self::format_results_array(initial_results)
            };
            return Ok((response, outcome));
        }

        // --- Phase 2: Block if necessary ---
        let timeout = self.block_timeout.unwrap();
        let stream_keys: Vec<Bytes> = self.streams.iter().map(|(k, _)| k.clone()).collect();

        let state = ctx.state.clone();
        let block_result = state
            .stream_blocker_manager
            .block_on(ctx, &stream_keys, timeout)
            .await;

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
                let (final_results, final_write_occurred) = self.read_from_streams(ctx).await?;
                let outcome = if final_write_occurred {
                    WriteOutcome::Write { keys_modified: 1 }
                } else {
                    WriteOutcome::DidNotWrite
                };
                let response = if final_results.is_empty() {
                    RespValue::Null
                } else {
                    Self::format_results_array(final_results)
                };
                Ok((response, outcome))
            }
        }
    }
}

impl XReadGroup {
    /// Formats the final result into the nested array structure expected by clients.
    fn format_results_array(results: Vec<RespValue>) -> RespValue {
        RespValue::Array(results)
    }

    /// The core logic to read entries from streams for a specific consumer group.
    async fn read_from_streams<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(Vec<RespValue>, bool), SpinelDBError> {
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "XREADGROUP requires multi-key lock".into(),
                ));
            }
        };
        let mut final_results = Vec::new();
        let mut any_write_occurred = false;

        for (key, id_spec) in &self.streams {
            let shard_index = ctx.db.get_shard_index(key);
            let guard = guards
                .get_mut(&shard_index)
                .ok_or_else(|| SpinelDBError::Internal("Missing shard lock".into()))?;
            let Some(entry) = guard.get_mut(key) else {
                continue;
            };
            if entry.is_expired() {
                continue;
            }
            if let DataValue::Stream(stream) = &mut entry.data {
                let Some(group) = stream.groups.get_mut(&self.group_name) else {
                    return Err(SpinelDBError::InvalidState(format!(
                        "-NOGROUP No such consumer group '{}' for key '{}'",
                        String::from_utf8_lossy(&self.group_name),
                        String::from_utf8_lossy(key)
                    )));
                };

                let mut stream_entries = Vec::new();
                let mut write_occurred_for_this_stream = false;

                // Case 1: Read pending entries for this consumer.
                if let GroupStreamIdSpec::Exact(start_id) = id_spec {
                    for (&id, pel_info) in group
                        .pending_entries
                        .range((Bound::Included(*start_id), Bound::Unbounded))
                    {
                        if pel_info.consumer_name == self.consumer_name
                            && let Some(stream_entry) = stream.entries.get(&id)
                        {
                            stream_entries.push(stream_entry.clone());
                            if stream_entries.len() >= self.count.unwrap_or(usize::MAX) {
                                break;
                            }
                        }
                    }
                }

                // Case 2: Read new entries from the stream.
                if let GroupStreamIdSpec::New = id_spec {
                    let start_id = group.last_delivered_id;
                    let range = stream
                        .entries
                        .range((Bound::Excluded(start_id), Bound::Unbounded));
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    let consumer = group
                        .consumers
                        .entry(self.consumer_name.clone())
                        .or_insert_with(|| Consumer {
                            name: self.consumer_name.clone(),
                            ..Default::default()
                        });
                    consumer.seen_time_ms = now_ms;

                    for (_, stream_entry) in range.take(self.count.unwrap_or(usize::MAX)) {
                        stream_entries.push(stream_entry.clone());
                        group.last_delivered_id = stream_entry.id;
                        write_occurred_for_this_stream = true;

                        if !self.noack {
                            consumer.pending_ids.insert(stream_entry.id);
                            group.idle_index.insert((now_ms, stream_entry.id));
                            group.pending_entries.insert(
                                stream_entry.id,
                                PendingEntryInfo {
                                    consumer_name: self.consumer_name.clone(),
                                    delivery_count: 1,
                                    delivery_time_ms: now_ms,
                                },
                            );
                        }
                    }
                }

                if !stream_entries.is_empty() {
                    let entry_values: Vec<RespValue> = stream_entries
                        .into_iter()
                        .map(|entry| XRead::format_entry(&entry.id, &entry))
                        .collect();
                    final_results.push(RespValue::Array(vec![
                        RespValue::BulkString(key.clone()),
                        RespValue::Array(entry_values),
                    ]));
                }

                if write_occurred_for_this_stream {
                    any_write_occurred = true;
                    entry.version += 1;
                }
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }
        Ok((final_results, any_write_occurred))
    }
}

impl CommandSpec for XReadGroup {
    fn name(&self) -> &'static str {
        "xreadgroup"
    }
    fn arity(&self) -> i64 {
        -7
    }
    fn flags(&self) -> CommandFlags {
        let mut flags = CommandFlags::WRITE | CommandFlags::MOVABLEKEYS;
        if self.block_timeout.is_some() {
            flags.insert(CommandFlags::NO_PROPAGATE);
        }
        flags
    }
    fn first_key(&self) -> i64 {
        0 // Keys are parsed specially.
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
        let mut args = vec![
            Bytes::from_static(b"GROUP"),
            self.group_name.clone(),
            self.consumer_name.clone(),
        ];
        if let Some(count) = self.count {
            args.extend([Bytes::from_static(b"COUNT"), count.to_string().into()]);
        }
        if let Some(block) = self.block_timeout {
            args.extend([
                Bytes::from_static(b"BLOCK"),
                block.as_millis().to_string().into(),
            ]);
        }
        if self.noack {
            args.push(Bytes::from_static(b"NOACK"));
        }
        args.push(Bytes::from_static(b"STREAMS"));

        let (keys, ids): (Vec<_>, Vec<_>) = self.streams.iter().cloned().unzip();
        args.extend(keys);
        args.extend(ids.iter().map(|id_spec| match id_spec {
            GroupStreamIdSpec::Exact(id) => id.to_string().into(),
            GroupStreamIdSpec::New => Bytes::from_static(b">"),
        }));
        args
    }
}
