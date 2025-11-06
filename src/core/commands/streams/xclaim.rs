// src/core/commands/streams/xclaim.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::commands::streams::xrange::XRange; // For format_entry
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::stream::{Consumer, PendingEntryInfo, StreamId};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct XClaim {
    key: Bytes,
    group: Bytes,
    consumer: Bytes,
    min_idle_time: u64,
    ids: Vec<StreamId>,
    justid: bool,
    idle: Option<u64>,
    time: Option<u64>,
    retrycount: Option<u64>,
    force: bool,
}

impl ParseCommand for XClaim {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 4 {
            return Err(SpinelDBError::WrongArgumentCount("XCLAIM".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let group = extract_bytes(&args[1])?;
        let consumer = extract_bytes(&args[2])?;
        let min_idle_time = extract_string(&args[3])?
            .parse()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        let mut ids = Vec::new();
        let mut justid = false;
        let mut idle = None;
        let mut time = None;
        let mut retrycount = None;
        let mut force = false;

        let mut i = 4;
        while i < args.len() {
            let arg_str = extract_string(&args[i])?.to_ascii_lowercase();
            match arg_str.as_str() {
                "justid" => {
                    justid = true;
                    i += 1;
                }
                "idle" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    idle = Some(extract_string(&args[i])?.parse()?);
                    i += 1;
                }
                "time" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    time = Some(extract_string(&args[i])?.parse()?);
                    i += 1;
                }
                "retrycount" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    retrycount = Some(extract_string(&args[i])?.parse()?);
                    i += 1;
                }
                "force" => {
                    force = true;
                    i += 1;
                }
                _ => {
                    // Assume remaining arguments are IDs
                    break;
                }
            }
        }

        // IDLE and TIME options are mutually exclusive.
        if idle.is_some() && time.is_some() {
            return Err(SpinelDBError::SyntaxError);
        }

        // Parse IDs after optional arguments
        while i < args.len() {
            let id_str = extract_string(&args[i])?;
            ids.push(
                id_str
                    .parse::<StreamId>()
                    .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?,
            );
            i += 1;
        }

        if ids.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "XCLAIM requires at least one ID".to_string(),
            ));
        }

        Ok(XClaim {
            key,
            group,
            consumer,
            min_idle_time,
            ids,
            justid,
            idle,
            time,
            retrycount,
            force,
        })
    }
}

#[async_trait]
impl ExecutableCommand for XClaim {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let entry = guard.get_mut(&self.key).ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::Stream(stream) = &mut entry.data {
            let group = stream.groups.get_mut(&self.group).ok_or_else(|| {
                SpinelDBError::InvalidState(format!(
                    "-NOGROUP No such consumer group '{}' for key '{}'",
                    String::from_utf8_lossy(&self.group),
                    String::from_utf8_lossy(&self.key)
                ))
            })?;

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let mut claimed_entries = Vec::new();

            // Calculate the new delivery time based on IDLE, TIME, or the current time.
            let new_delivery_time = if let Some(idle_ms) = self.idle {
                now_ms.saturating_sub(idle_ms)
            } else if let Some(time_ms) = self.time {
                time_ms
            } else {
                now_ms
            };

            for id in &self.ids {
                if let Some(pel_info) = group.pending_entries.get_mut(id) {
                    // Check min_idle_time only if not forced.
                    if self.force
                        || now_ms.saturating_sub(pel_info.delivery_time_ms) >= self.min_idle_time
                    {
                        // Remove old entry from the idle index before updating.
                        group.idle_index.remove(&(pel_info.delivery_time_ms, *id));

                        // Remove from old consumer's list.
                        if let Some(old_consumer) = group.consumers.get_mut(&pel_info.consumer_name)
                        {
                            old_consumer.pending_ids.remove(id);
                        }

                        // Update PEL info for the new consumer.
                        pel_info.consumer_name = self.consumer.clone();
                        pel_info.delivery_time_ms = new_delivery_time;
                        pel_info.delivery_count =
                            self.retrycount.unwrap_or(pel_info.delivery_count + 1);

                        // Re-insert into the idle index with the new timestamp.
                        group.idle_index.insert((new_delivery_time, *id));

                        // Update new consumer's state.
                        let new_consumer = group
                            .consumers
                            .entry(self.consumer.clone())
                            .or_insert_with(|| Consumer {
                                name: self.consumer.clone(),
                                seen_time_ms: now_ms,
                                pending_ids: Default::default(),
                            });
                        new_consumer.pending_ids.insert(*id);

                        if let Some(stream_entry) = stream.entries.get(id) {
                            claimed_entries.push(stream_entry.clone());
                        }
                    }
                } else if self.force {
                    // If FORCE is true, and entry is not in PEL, claim it directly.
                    if let Some(stream_entry) = stream.entries.get(id) {
                        let new_consumer = group
                            .consumers
                            .entry(self.consumer.clone())
                            .or_insert_with(|| Consumer {
                                name: self.consumer.clone(),
                                seen_time_ms: now_ms,
                                pending_ids: Default::default(),
                            });
                        new_consumer.pending_ids.insert(*id);

                        // Add to idle_index when forcing a claim.
                        group.idle_index.insert((new_delivery_time, *id));

                        group.pending_entries.insert(
                            *id,
                            PendingEntryInfo {
                                consumer_name: self.consumer.clone(),
                                delivery_count: self.retrycount.unwrap_or(1),
                                delivery_time_ms: new_delivery_time,
                            },
                        );
                        claimed_entries.push(stream_entry.clone());
                    }
                }
            }

            if claimed_entries.is_empty() {
                return Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite));
            }

            entry.version += 1;

            let response_values = if self.justid {
                claimed_entries
                    .iter()
                    .map(|e| RespValue::BulkString(e.id.to_string().into()))
                    .collect()
            } else {
                claimed_entries
                    .iter()
                    .map(|e| XRange::format_entry((&e.id, e)))
                    .collect()
            };

            Ok((
                RespValue::Array(response_values),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for XClaim {
    fn name(&self) -> &'static str {
        "xclaim"
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
            self.group.clone(),
            self.consumer.clone(),
            self.min_idle_time.to_string().into(),
        ];
        // Note: The order of optional arguments is important for correct replication.
        // We serialize them before the IDs.
        if let Some(idle_time) = self.idle {
            args.push(Bytes::from_static(b"IDLE"));
            args.push(idle_time.to_string().into());
        }
        if let Some(time) = self.time {
            args.push(Bytes::from_static(b"TIME"));
            args.push(time.to_string().into());
        }
        if let Some(retrycount) = self.retrycount {
            args.push(Bytes::from_static(b"RETRYCOUNT"));
            args.push(retrycount.to_string().into());
        }
        if self.force {
            args.push(Bytes::from_static(b"FORCE"));
        }
        if self.justid {
            args.push(Bytes::from_static(b"JUSTID"));
        }
        // IDs are always last.
        args.extend(self.ids.iter().map(|id| id.to_string().into()));

        args
    }
}
