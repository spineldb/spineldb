// src/core/commands/streams/xpending.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::stream::{ConsumerGroup, StreamId};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub enum XPendingSubcommand {
    Summary {
        key: Bytes,
        group_name: Bytes,
    },
    Detailed {
        key: Bytes,
        group_name: Bytes,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<Bytes>,
        idle_time_filter: Option<u64>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct XPending {
    pub subcommand: XPendingSubcommand,
}

impl XPending {
    fn get_key(&self) -> &Bytes {
        match &self.subcommand {
            XPendingSubcommand::Summary { key, .. } => key,
            XPendingSubcommand::Detailed { key, .. } => key,
        }
    }
}

impl Default for XPendingSubcommand {
    fn default() -> Self {
        XPendingSubcommand::Summary {
            key: Default::default(),
            group_name: Default::default(),
        }
    }
}

impl ParseCommand for XPending {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("XPENDING".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let group_name = extract_bytes(&args[1])?;

        if args.len() == 2 {
            return Ok(XPending {
                subcommand: XPendingSubcommand::Summary { key, group_name },
            });
        }

        let mut i = 2;
        let mut idle_time_filter: Option<u64> = None;

        // Parse optional IDLE argument
        while i < args.len() {
            let arg_str = extract_string(&args[i])?.to_ascii_lowercase();
            match arg_str.as_str() {
                "idle" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    idle_time_filter = Some(extract_string(&args[i])?.parse()?);
                    i += 1;
                }
                _ => break, // End of options, start parsing range
            }
        }

        // Gunakan .parse() karena StreamId sekarang mengimplementasikan FromStr
        let start = extract_string(&args[i])?
            .parse::<StreamId>()
            .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?;
        i += 1;
        let end = extract_string(&args[i])?
            .parse::<StreamId>()
            .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?;
        i += 1;
        let count = extract_string(&args[i])?.parse::<usize>()?;
        i += 1;

        let consumer = if i < args.len() {
            Some(extract_bytes(&args[i])?)
        } else {
            None
        };

        Ok(XPending {
            subcommand: XPendingSubcommand::Detailed {
                key,
                group_name,
                start,
                end,
                count,
                consumer,
                idle_time_filter,
            },
        })
    }
}

#[async_trait]
impl ExecutableCommand for XPending {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        let key = self.get_key();
        if let Some(entry) = guard.peek(key) {
            if entry.is_expired() {
                return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
            }

            if let DataValue::Stream(stream) = &entry.data {
                let group_name = match &self.subcommand {
                    XPendingSubcommand::Summary { group_name, .. } => group_name,
                    XPendingSubcommand::Detailed { group_name, .. } => group_name,
                };

                if let Some(group) = stream.groups.get(group_name) {
                    return match &self.subcommand {
                        XPendingSubcommand::Summary { .. } => self.execute_summary(group),
                        XPendingSubcommand::Detailed {
                            start,
                            end,
                            count,
                            consumer,
                            idle_time_filter,
                            ..
                        } => self.execute_detailed(
                            group,
                            *start,
                            *end,
                            *count,
                            consumer.as_ref(),
                            *idle_time_filter,
                        ),
                    };
                }
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }

        Ok((RespValue::Null, WriteOutcome::DidNotWrite))
    }
}

impl XPending {
    fn execute_summary(
        &self,
        group: &ConsumerGroup,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let pending_count = group.pending_entries.len() as i64;
        let first_id = group
            .pending_entries
            .keys()
            .next()
            .map(|id| RespValue::BulkString(id.to_string().into()))
            .unwrap_or(RespValue::Null);
        let last_id = group
            .pending_entries
            .keys()
            .next_back()
            .map(|id| RespValue::BulkString(id.to_string().into()))
            .unwrap_or(RespValue::Null);

        let consumers_info: Vec<RespValue> = group
            .consumers
            .values()
            .filter(|c| !c.pending_ids.is_empty())
            .map(|c| {
                RespValue::Array(vec![
                    RespValue::BulkString(c.name.clone()),
                    RespValue::BulkString(c.pending_ids.len().to_string().into()),
                ])
            })
            .collect();

        let response = RespValue::Array(vec![
            RespValue::Integer(pending_count),
            first_id,
            last_id,
            RespValue::Array(consumers_info),
        ]);

        Ok((response, WriteOutcome::DidNotWrite))
    }

    fn execute_detailed(
        &self,
        group: &ConsumerGroup,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer_filter: Option<&Bytes>,
        idle_time_filter: Option<u64>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut results = Vec::new();
        let range = group
            .pending_entries
            .range((Bound::Included(start), Bound::Included(end)));
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        for (id, pel_info) in range {
            if let Some(filter) = consumer_filter
                && pel_info.consumer_name != *filter
            {
                continue;
            }

            let idle_time = now_ms.saturating_sub(pel_info.delivery_time_ms as u128) as u64;
            if let Some(min_idle) = idle_time_filter
                && idle_time < min_idle
            {
                continue;
            }

            results.push(RespValue::Array(vec![
                RespValue::BulkString(id.to_string().into()),
                RespValue::BulkString(pel_info.consumer_name.clone()),
                RespValue::Integer(idle_time as i64),
                RespValue::Integer(pel_info.delivery_count as i64),
            ]));

            if results.len() >= count {
                break;
            }
        }

        Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for XPending {
    fn name(&self) -> &'static str {
        "xpending"
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
        vec![self.get_key().clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![]
    }
}
