// src/core/commands/set/smove.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct Smove {
    pub source: Bytes,
    pub destination: Bytes,
    pub member: Bytes,
}

impl ParseCommand for Smove {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "SMOVE")?;
        Ok(Smove {
            source: extract_bytes(&args[0])?,
            destination: extract_bytes(&args[1])?,
            member: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Smove {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "SMOVE requires multi-key lock".into(),
                ));
            }
        };
        let source_shard_index = ctx.db.get_shard_index(&self.source);
        let dest_shard_index = ctx.db.get_shard_index(&self.destination);

        // --- Step 1: Check if the member exists in the source set and is a valid Set ---
        let member_exists_in_source = {
            let source_guard =
                guards
                    .get_mut(&source_shard_index)
                    .ok_or(SpinelDBError::Internal(
                        "Missing source lock for SMOVE".into(),
                    ))?;

            if let Some(source_entry) = source_guard.get_mut(&self.source) {
                if source_entry.is_expired() {
                    false // Expired key is treated as non-existent.
                } else if let DataValue::Set(set) = &mut source_entry.data {
                    set.contains(&self.member)
                } else {
                    return Err(SpinelDBError::WrongType);
                }
            } else {
                false // Source key doesn't exist.
            }
        };

        if !member_exists_in_source {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        // --- Step 2: Add the member to the destination set (safer to add first) ---
        // This ensures that if a crash happens, the member exists in at least one set,
        // preventing data loss.
        {
            let dest_guard = guards
                .get_mut(&dest_shard_index)
                .ok_or(SpinelDBError::Internal(
                    "Missing destination lock for SMOVE".into(),
                ))?;
            let dest_entry = dest_guard.get_or_insert_with_mut(self.destination.clone(), || {
                StoredValue::new(DataValue::Set(HashSet::new()))
            });

            if let DataValue::Set(set) = &mut dest_entry.data {
                // If the member was not already in the destination set, update metadata.
                if set.insert(self.member.clone()) {
                    let mem_added = self.member.len();
                    dest_entry.size += mem_added;
                    dest_entry.version += 1;
                    ctx.db
                        .get_shard(dest_shard_index)
                        .update_memory(mem_added as isize);
                }
            } else {
                // If the destination exists but is not a set, fail the operation.
                // The source set remains untouched. This is the correct behavior.
                return Err(SpinelDBError::WrongType);
            }
        }

        // --- Step 3: Remove the member from the source set (now that it's safe in destination) ---
        {
            let source_guard =
                guards
                    .get_mut(&source_shard_index)
                    .ok_or(SpinelDBError::Internal(
                        "Missing source lock for SMOVE final step".into(),
                    ))?;

            // We know the entry exists from the initial check.
            let source_entry = source_guard.get_mut(&self.source).unwrap();

            if let DataValue::Set(set) = &mut source_entry.data {
                // This remove operation should always succeed because we checked for existence.
                if set.remove(&self.member) {
                    let mem_freed = self.member.len();
                    source_entry.size -= mem_freed;
                    source_entry.version += 1;
                    ctx.db
                        .get_shard(source_shard_index)
                        .update_memory(-(mem_freed as isize));

                    // If the source set becomes empty, remove the key entirely.
                    if set.is_empty() {
                        source_guard.pop(&self.source);
                    }
                }
            }
        }

        Ok((
            RespValue::Integer(1),
            WriteOutcome::Write { keys_modified: 2 },
        ))
    }
}

impl CommandSpec for Smove {
    fn name(&self) -> &'static str {
        "smove"
    }
    fn arity(&self) -> i64 {
        4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
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
        vec![
            self.source.clone(),
            self.destination.clone(),
            self.member.clone(),
        ]
    }
}
