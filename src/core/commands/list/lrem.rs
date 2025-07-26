// src/core/commands/list/lrem.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

/// Represents the `LREM` command, which removes elements from a list.
#[derive(Debug, Clone, Default)]
pub struct LRem {
    /// The key of the list.
    pub key: Bytes,
    /// The number of occurrences to remove.
    /// - `count > 0`: Remove elements from head to tail.
    /// - `count < 0`: Remove elements from tail to head.
    /// - `count = 0`: Remove all matching elements.
    pub count: i64,
    /// The element to remove.
    pub element: Bytes,
}

impl ParseCommand for LRem {
    /// Parses the arguments for the LREM command.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "LREM")?;
        Ok(LRem {
            key: extract_bytes(&args[0])?,
            count: extract_string(&args[1])?.parse()?,
            element: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for LRem {
    /// Executes the LREM command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let DataValue::List(list) = &mut entry.data else {
            return Err(SpinelDBError::WrongType);
        };

        let mut removed_count = 0;
        let mut mem_freed = 0;
        let element_len = self.element.len();

        if self.count > 0 {
            // Remove from head to tail using an efficient in-place retain.
            let limit = self.count;
            list.retain(|val| {
                if removed_count < limit && val == &self.element {
                    removed_count += 1;
                    mem_freed += element_len;
                    false // Remove this element.
                } else {
                    true // Keep this element.
                }
            });
        } else if self.count < 0 {
            // Remove from tail to head using an O(N) approach to avoid O(N*M) performance.
            let limit = self.count.abs();
            let mut new_list = VecDeque::with_capacity(list.len());
            for item in list.iter().rev() {
                if removed_count < limit && item == &self.element {
                    removed_count += 1;
                    mem_freed += element_len;
                } else {
                    new_list.push_front(item.clone());
                }
            }
            *list = new_list;
        } else {
            // count == 0: remove all occurrences.
            let original_len = list.len();
            list.retain(|val| {
                if val == &self.element {
                    mem_freed += element_len;
                    false
                } else {
                    true
                }
            });
            removed_count = (original_len - list.len()) as i64;
        }

        if removed_count > 0 {
            let is_now_empty = list.is_empty();
            entry.version = entry.version.wrapping_add(1);
            entry.size -= mem_freed;
            shard.current_memory.fetch_sub(mem_freed, Ordering::Relaxed);

            let outcome = if is_now_empty {
                shard_cache_guard.pop(&self.key);
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::Write { keys_modified: 1 }
            };

            Ok((RespValue::Integer(removed_count), outcome))
        } else {
            Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
        }
    }
}

impl CommandSpec for LRem {
    fn name(&self) -> &'static str {
        "lrem"
    }
    fn arity(&self) -> i64 {
        4
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
        vec![
            self.key.clone(),
            self.count.to_string().into(),
            self.element.clone(),
        ]
    }
}
