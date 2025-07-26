// src/core/commands/streams/xinfo.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::stream::{Stream, StreamEntry};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub enum XInfoSubcommand {
    Stream { full: bool },
    Groups,
    Consumers { group_name: Bytes },
}

#[derive(Debug, Clone)]
pub struct XInfo {
    pub key: Bytes,
    pub subcommand: XInfoSubcommand,
}

impl Default for XInfo {
    fn default() -> Self {
        Self {
            key: Default::default(),
            subcommand: XInfoSubcommand::Stream { full: false },
        }
    }
}

impl ParseCommand for XInfo {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("XINFO".to_string()));
        }

        let subcommand_str = extract_string(&args[0])?.to_ascii_lowercase();
        let key = extract_bytes(&args[1])?;

        let subcommand = match subcommand_str.as_str() {
            "stream" => {
                let mut full = false;
                if args.len() > 2 {
                    if args.len() == 3 && extract_string(&args[2])?.eq_ignore_ascii_case("full") {
                        full = true;
                    } else {
                        return Err(SpinelDBError::SyntaxError);
                    }
                }
                XInfoSubcommand::Stream { full }
            }
            "groups" => XInfoSubcommand::Groups,
            "consumers" => {
                if args.len() < 3 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "XINFO CONSUMERS".to_string(),
                    ));
                }
                let group_name = extract_bytes(&args[2])?;
                XInfoSubcommand::Consumers { group_name }
            }
            _ => return Err(SpinelDBError::SyntaxError),
        };

        Ok(XInfo { key, subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for XInfo {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, guard) = ctx.get_single_shard_context_mut()?;

        let entry = guard.get(&self.key).ok_or(SpinelDBError::KeyNotFound)?;
        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::Stream(stream) = &entry.data {
            let response = match &self.subcommand {
                XInfoSubcommand::Stream { full } => {
                    if *full {
                        build_full_stream_info_response(stream)?
                    } else {
                        build_stream_info_response(stream)?
                    }
                }
                XInfoSubcommand::Groups => build_groups_info_response(stream)?,
                XInfoSubcommand::Consumers { group_name } => {
                    build_consumers_info_response(stream, group_name)?
                }
            };
            Ok((response, WriteOutcome::DidNotWrite))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for XInfo {
    fn name(&self) -> &'static str {
        "xinfo"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        2
    }
    fn last_key(&self) -> i64 {
        2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![];
        match &self.subcommand {
            XInfoSubcommand::Stream { full } => {
                args.push("STREAM".into());
                args.push(self.key.clone());
                if *full {
                    args.push("FULL".into());
                }
            }
            XInfoSubcommand::Groups => {
                args.push("GROUPS".into());
                args.push(self.key.clone());
            }
            XInfoSubcommand::Consumers { group_name } => {
                args.push("CONSUMERS".into());
                args.push(self.key.clone());
                args.push(group_name.clone());
            }
        }
        args
    }
}

fn format_stream_entry(entry: &StreamEntry) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(entry.id.to_string().into()),
        RespValue::Array(
            entry
                .fields
                .iter()
                .flat_map(|(k, v)| {
                    vec![
                        RespValue::BulkString(k.clone()),
                        RespValue::BulkString(v.clone()),
                    ]
                })
                .collect(),
        ),
    ])
}

fn build_stream_info_response(stream: &Stream) -> Result<RespValue, SpinelDBError> {
    let mut info = vec![
        RespValue::BulkString("length".into()),
        RespValue::Integer(stream.length as i64),
        RespValue::BulkString("radix-tree-keys".into()),
        RespValue::Integer(stream.entries.len() as i64),
        RespValue::BulkString("radix-tree-nodes".into()),
        RespValue::Integer((stream.entries.len() as f64 / 10.0).ceil() as i64 + 1),
        RespValue::BulkString("groups".into()),
        RespValue::Integer(stream.groups.len() as i64),
        RespValue::BulkString("last-generated-id".into()),
        RespValue::BulkString(stream.last_generated_id.to_string().into()),
    ];

    if let Some(first_entry) = stream.entries.values().next() {
        info.push(RespValue::BulkString("first-entry".into()));
        info.push(format_stream_entry(first_entry));
    } else {
        info.push(RespValue::BulkString("first-entry".into()));
        info.push(RespValue::Null);
    }

    if let Some(last_entry) = stream.entries.values().last() {
        info.push(RespValue::BulkString("last-entry".into()));
        info.push(format_stream_entry(last_entry));
    } else {
        info.push(RespValue::BulkString("last-entry".into()));
        info.push(RespValue::Null);
    }

    Ok(RespValue::Array(info))
}

fn build_groups_info_response(stream: &Stream) -> Result<RespValue, SpinelDBError> {
    let groups_array = stream
        .groups
        .values()
        .map(|group| {
            RespValue::Array(vec![
                RespValue::BulkString("name".into()),
                RespValue::BulkString(group.name.clone()),
                RespValue::BulkString("consumers".into()),
                RespValue::Integer(group.consumers.len() as i64),
                RespValue::BulkString("pending".into()),
                RespValue::Integer(group.pending_entries.len() as i64),
                RespValue::BulkString("last-delivered-id".into()),
                RespValue::BulkString(group.last_delivered_id.to_string().into()),
            ])
        })
        .collect();
    Ok(RespValue::Array(groups_array))
}

fn build_consumers_info_response(
    stream: &Stream,
    group_name: &Bytes,
) -> Result<RespValue, SpinelDBError> {
    let group = stream
        .groups
        .get(group_name)
        .ok_or(SpinelDBError::ConsumerGroupNotFound)?;

    let consumers_array = group
        .consumers
        .values()
        .map(|consumer| {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let idle_time = now_ms.saturating_sub(consumer.seen_time_ms);
            RespValue::Array(vec![
                RespValue::BulkString("name".into()),
                RespValue::BulkString(consumer.name.clone()),
                RespValue::BulkString("pending".into()),
                RespValue::Integer(consumer.pending_ids.len() as i64),
                RespValue::BulkString("idle".into()),
                RespValue::Integer(idle_time as i64),
            ])
        })
        .collect();
    Ok(RespValue::Array(consumers_array))
}

fn build_full_stream_info_response(stream: &Stream) -> Result<RespValue, SpinelDBError> {
    let mut info = vec![
        RespValue::BulkString("length".into()),
        RespValue::Integer(stream.length as i64),
        RespValue::BulkString("radix-tree-keys".into()),
        RespValue::Integer(stream.entries.len() as i64),
        RespValue::BulkString("radix-tree-nodes".into()),
        RespValue::Integer((stream.entries.len() as f64 / 10.0).ceil() as i64 + 1),
        RespValue::BulkString("last-generated-id".into()),
        RespValue::BulkString(stream.last_generated_id.to_string().into()),
        RespValue::BulkString("entries".into()),
        RespValue::Array(stream.entries.values().map(format_stream_entry).collect()),
    ];

    // Consumer Groups Info
    let groups_array = stream
        .groups
        .values()
        .map(|group| {
            let consumers_array = group
                .consumers
                .values()
                .map(|consumer| {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    let idle_time = now_ms.saturating_sub(consumer.seen_time_ms);
                    RespValue::Array(vec![
                        RespValue::BulkString("name".into()),
                        RespValue::BulkString(consumer.name.clone()),
                        RespValue::BulkString("pending".into()),
                        RespValue::Integer(consumer.pending_ids.len() as i64),
                        RespValue::BulkString("idle".into()),
                        RespValue::Integer(idle_time as i64),
                    ])
                })
                .collect();

            RespValue::Array(vec![
                RespValue::BulkString("name".into()),
                RespValue::BulkString(group.name.clone()),
                RespValue::BulkString("consumers".into()),
                RespValue::Array(consumers_array),
                RespValue::BulkString("pending".into()),
                RespValue::Integer(group.pending_entries.len() as i64),
                RespValue::BulkString("last-delivered-id".into()),
                RespValue::BulkString(group.last_delivered_id.to_string().into()),
            ])
        })
        .collect();
    info.push(RespValue::BulkString("groups".into()));
    info.push(RespValue::Array(groups_array));

    Ok(RespValue::Array(info))
}
