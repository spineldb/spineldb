// src/core/commands/streams/xautoclaim.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::stream::{Consumer, StreamId}; // Import Consumer
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct XAutoClaim {
    pub key: Bytes,
    pub group_name: Bytes,
    pub consumer_name: Bytes,
    pub min_idle_time: Duration,
    pub start_id: StreamId,
    pub count: Option<usize>,
    pub justid: bool,
}

impl ParseCommand for XAutoClaim {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 5 {
            return Err(SpinelDBError::WrongArgumentCount("XAUTOCLAIM".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let group_name = extract_bytes(&args[1])?;
        let consumer_name = extract_bytes(&args[2])?;
        let min_idle_time_ms: u64 = extract_string(&args[3])?.parse()?;
        let start_id_str = extract_string(&args[4])?;

        let start_id = if start_id_str == "0-0" {
            StreamId::new(0, 0)
        } else {
            start_id_str
                .parse::<StreamId>()
                .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?
        };

        let mut cmd = XAutoClaim {
            key,
            group_name,
            consumer_name,
            min_idle_time: Duration::from_millis(min_idle_time_ms),
            start_id,
            count: None,
            justid: false,
        };

        let mut i = 5;
        while i < args.len() {
            let arg_str = extract_string(&args[i])?.to_ascii_lowercase();
            match arg_str.as_str() {
                "count" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    cmd.count = Some(extract_string(&args[i])?.parse()?);
                    i += 1;
                }
                "justid" => {
                    cmd.justid = true;
                    i += 1;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for XAutoClaim {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, guard) = ctx.get_single_shard_context_mut()?;

        let entry = guard.get_mut(&self.key).ok_or(SpinelDBError::KeyNotFound)?;
        if entry.is_expired() {
            return Ok((
                RespValue::Array(vec![
                    RespValue::BulkString(self.start_id.to_string().into()),
                    RespValue::Array(vec![]),
                ]),
                WriteOutcome::DidNotWrite,
            ));
        }

        if let DataValue::Stream(stream) = &mut entry.data {
            let group = stream
                .groups
                .get_mut(&self.group_name)
                .ok_or(SpinelDBError::ConsumerGroupNotFound)?;

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let idle_time_boundary = now_ms.saturating_sub(self.min_idle_time.as_millis() as u64);
            let mut ids_to_claim = Vec::new();

            // Find candidate entries from the idle index, which is much more efficient than scanning the whole PEL.
            for &(_delivery_time, id) in group
                .idle_index
                .range(..=(idle_time_boundary, StreamId::new(u64::MAX, u64::MAX)))
            {
                if id >= self.start_id {
                    ids_to_claim.push(id);
                    if let Some(count) = self.count
                        && ids_to_claim.len() >= count
                    {
                        break;
                    }
                }
            }

            if ids_to_claim.is_empty() {
                return Ok((
                    RespValue::Array(vec![
                        RespValue::BulkString(self.start_id.to_string().into()),
                        RespValue::Array(vec![]),
                    ]),
                    WriteOutcome::DidNotWrite,
                ));
            }

            let next_scan_id = ids_to_claim.last().map_or(self.start_id, |id| *id);
            let mut claimed_entries_data = Vec::new();

            // Perform the claiming process for all identified entries.
            for id in &ids_to_claim {
                if let Some(pel_info) = group.pending_entries.get_mut(id) {
                    // Store the old consumer name before updating.
                    let old_consumer_name = pel_info.consumer_name.clone();

                    // 1. Remove old entry from the idle index.
                    group.idle_index.remove(&(pel_info.delivery_time_ms, *id));

                    // 2. Update PEL info for the new consumer.
                    pel_info.delivery_time_ms = now_ms;
                    pel_info.delivery_count += 1;
                    pel_info.consumer_name = self.consumer_name.clone();

                    // 3. Re-insert into the idle index with the new timestamp.
                    group.idle_index.insert((now_ms, *id));

                    // 4. Update the old consumer's state.
                    if let Some(old_consumer) = group.consumers.get_mut(&old_consumer_name) {
                        old_consumer.pending_ids.remove(id);
                    }

                    // 5. Update the new consumer's state.
                    let new_consumer = group
                        .consumers
                        .entry(self.consumer_name.clone())
                        .or_insert_with(|| Consumer {
                            name: self.consumer_name.clone(),
                            ..Default::default()
                        });
                    new_consumer.seen_time_ms = now_ms;
                    new_consumer.pending_ids.insert(*id);

                    // 6. Collect the actual entry data for the response.
                    if let Some(entry_data) = stream.entries.get(id) {
                        claimed_entries_data.push(entry_data.clone());
                    }
                }
            }

            // Mark that a modification occurred.
            entry.version += 1;

            let (next_id_str, entries_resp_array) = if self.justid {
                let ids_array = ids_to_claim
                    .into_iter()
                    .map(|id| RespValue::BulkString(id.to_string().into()))
                    .collect();
                (next_scan_id.to_string(), RespValue::Array(ids_array))
            } else {
                let formatted_entries: Vec<RespValue> = claimed_entries_data
                    .iter()
                    .map(|e| {
                        crate::core::commands::streams::xrange::XRange::format_entry((&e.id, e))
                    })
                    .collect();
                (
                    next_scan_id.to_string(),
                    RespValue::Array(formatted_entries),
                )
            };

            let response = RespValue::Array(vec![
                RespValue::BulkString(next_id_str.into()),
                entries_resp_array,
            ]);

            Ok((response, WriteOutcome::Write { keys_modified: 1 }))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for XAutoClaim {
    fn name(&self) -> &'static str {
        "xautoclaim"
    }

    fn arity(&self) -> i64 {
        -5
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        let mut args = vec![
            self.key.clone(),
            self.group_name.clone(),
            self.consumer_name.clone(),
            self.min_idle_time.as_millis().to_string().into(),
            self.start_id.to_string().into(),
        ];
        if let Some(count) = self.count {
            args.push(Bytes::from_static(b"COUNT"));
            args.push(count.to_string().into());
        }
        if self.justid {
            args.push(Bytes::from_static(b"JUSTID"));
        }
        args
    }
}
