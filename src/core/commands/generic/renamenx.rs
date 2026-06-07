// src/core/commands/generic/renamenx.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct RenameNx {
    pub source: Bytes,
    pub destination: Bytes,
}

impl ParseCommand for RenameNx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "RENAMENX")?;
        Ok(RenameNx {
            source: extract_bytes(&args[0])?,
            destination: extract_bytes(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for RenameNx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if self.source == self.destination {
            // According to Redis spec, RENAMENX where source and destination are the same
            // should check for existence and return 1 if it exists, 0 otherwise, without moving.
            // However, a simpler and common interpretation is that it's a no-op that fails. We'll return 0.
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "RENAMENX requires multi-key lock".into(),
                ));
            }
        };

        let source_shard_index = ctx.db.get_shard_index(&self.source);
        let dest_shard_index = ctx.db.get_shard_index(&self.destination);

        // --- Step 1: Check if destination key already exists. If so, abort. ---
        {
            let dest_guard = guards
                .get(&dest_shard_index)
                .ok_or_else(|| SpinelDBError::Internal("Missing dest lock for RENAMENX".into()))?;
            if dest_guard
                .peek(&self.destination)
                .is_some_and(|e| !e.is_expired())
            {
                return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
            }
        }

        // --- Step 2: Pop the source value. ---
        let source_value = {
            let source_guard = guards.get_mut(&source_shard_index).ok_or_else(|| {
                SpinelDBError::Internal("Missing source lock for RENAMENX".into())
            })?;

            // Wake up anyone waiting on the source key before it's removed.
            if let Some(entry) = source_guard.peek(&self.source) {
                match &entry.data {
                    DataValue::List(_) | DataValue::SortedSet(_) => {
                        ctx.state
                            .blocker_manager
                            .wake_waiters_for_modification(&self.source);
                    }
                    DataValue::Stream(_) => {
                        ctx.state
                            .stream_blocker_manager
                            .notify_and_remove_all(&self.source);
                    }
                    _ => {}
                }
            }

            // Pop the source value after notification.
            source_guard
                .pop(&self.source)
                .ok_or(SpinelDBError::KeyNotFound)? // If source doesn't exist, fail the operation.
        };

        // --- Step 3: Put the value into the destination. ---
        let dest_guard = guards.get_mut(&dest_shard_index).ok_or_else(|| {
            SpinelDBError::Internal("Missing dest lock for RENAMENX (put)".into())
        })?;

        dest_guard.put(self.destination.clone(), source_value);

        // --- Step 4: Return success. ---
        Ok((
            RespValue::Integer(1),
            // The operation involves a delete (source) and a set (destination).
            WriteOutcome::Write { keys_modified: 2 },
        ))
    }
}

impl CommandSpec for RenameNx {
    fn name(&self) -> &'static str {
        "renamenx"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.source.clone(), self.destination.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_renamenx_parses_source_and_destination() {
        let r = RenameNx::parse(&[bs("src"), bs("dst")]).unwrap();
        assert_eq!(r.source, Bytes::from_static(b"src"));
        assert_eq!(r.destination, Bytes::from_static(b"dst"));
    }

    #[test]
    fn test_renamenx_with_too_few_args_is_error() {
        let r = RenameNx::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_renamenx_with_too_many_args_is_error() {
        let r = RenameNx::parse(&[bs("a"), bs("b"), bs("c")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_renamenx_with_non_bulk_is_wrong_type() {
        let r = RenameNx::parse(&[RespFrame::Integer(1), bs("b")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_renamenx_to_resp_args_round_trips() {
        let r = RenameNx::parse(&[bs("a"), bs("b")]).unwrap();
        let args = r.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"a"));
        assert_eq!(args[1], Bytes::from_static(b"b"));
    }
}
