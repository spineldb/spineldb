// src/core/commands/streams/xack.rs

//! Implements the `XACK` command for acknowledging processed stream messages
//! within a consumer group.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::stream::StreamId;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Represents the `XACK` command with its parsed arguments.
#[derive(Debug, Clone, Default)]
pub struct XAck {
    key: Bytes,
    group_name: Bytes,
    ids: Vec<StreamId>,
}

impl ParseCommand for XAck {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("XACK".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let group_name = extract_bytes(&args[1])?;
        let ids = args[2..]
            .iter()
            .map(|frame| {
                extract_string(frame)?
                    .parse::<StreamId>()
                    .map_err(|e| SpinelDBError::InvalidState(e.to_string()))
            })
            .collect::<Result<_, _>>()?;

        Ok(XAck {
            key,
            group_name,
            ids,
        })
    }
}

#[async_trait]
impl ExecutableCommand for XAck {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let mut acked_count = 0;

        let entry = guard.get_mut(&self.key).ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::Stream(stream) = &mut entry.data {
            let group = stream.groups.get_mut(&self.group_name).ok_or_else(|| {
                SpinelDBError::InvalidState(format!(
                    "-NOGROUP No such consumer group '{}' for key '{}'",
                    String::from_utf8_lossy(&self.group_name),
                    String::from_utf8_lossy(&self.key)
                ))
            })?;

            for id in &self.ids {
                // `remove` from the BTreeMap (PEL) returns the value if the key existed.
                if let Some(pel_info) = group.pending_entries.remove(id) {
                    acked_count += 1;

                    // Also remove the entry from the secondary idle index.
                    group.idle_index.remove(&(pel_info.delivery_time_ms, *id));

                    // Remove the ID from the specific consumer's pending list.
                    if let Some(consumer) = group.consumers.get_mut(&pel_info.consumer_name) {
                        consumer.pending_ids.remove(id);
                    }
                }
            }

            if acked_count > 0 {
                // Any modification to the stream state requires a version bump.
                entry.version += 1;
                Ok((
                    RespValue::Integer(acked_count),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for XAck {
    fn name(&self) -> &'static str {
        "xack"
    }
    fn arity(&self) -> i64 {
        -4
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
        let mut args = vec![self.key.clone(), self.group_name.clone()];
        args.extend(self.ids.iter().map(|id| id.to_string().into()));
        args
    }
}
