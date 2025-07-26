// src/core/commands/streams/xadd.rs

//! Implements the `XADD` command for adding entries to a stream.
//!
//! The `XADD` command is the fundamental write operation for the Stream data type.
//! It appends a new entry, consisting of one or more field-value pairs, to a specified stream.
//! Each entry is assigned a unique, monotonically increasing ID.
//!
//! # Command Syntax
//! `XADD key [NOMKSTREAM] [MAXLEN|MINID [= | ~] count] <* | id> field value [field value ...]`
//!
//! ## Options
//! - **key**: The name of the stream.
//! - **NOMKSTREAM**: If specified, the command will not create the stream if it doesn't already exist.
//! - **MAXLEN**: Trims the stream to a specified number of entries.
//! - **MINID**: Trims the stream to entries with an ID greater than or equal to the specified ID.
//! - **~ (approximate trimming)**: Can be used with `MAXLEN` or `MINID` for more efficient, but not perfectly exact, trimming.
//! - **id | \***: The ID for the new entry. `*` directs the server to auto-generate an ID.
//! - **field value ...**: One or more pairs of field names and their corresponding values that make up the entry.
//!
//! # Return Value
//! - On success, returns a Bulk String representing the unique ID of the newly added entry.
//! - If `NOMKSTREAM` is used and the stream does not exist, returns a Null reply.
//! - Returns an error if the key holds a value that is not a stream, or if the specified ID is invalid.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::{DbShard, ExecutionContext, ShardCache};
use crate::core::storage::stream::{Stream, StreamId};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use indexmap::IndexMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::MutexGuard;

/// Represents the parsed options for the `XADD` command.
#[derive(Debug, Clone, Default)]
pub struct XAddOptions {
    /// The ID to be used for the new entry. `None` corresponds to `*` (auto-generation).
    pub id_spec: Option<StreamId>,
    /// The collection of field-value pairs for the new entry.
    pub fields: IndexMap<Bytes, Bytes>,
    /// The trimming strategy, if specified (e.g., `MAXLEN`).
    pub maxlen: Option<(bool, usize)>, // (is_approximate, count)
    /// If `true`, the stream will not be created if it doesn't exist.
    pub nomkstream: bool,
}

/// Represents the `XADD` command with its key and parsed options.
#[derive(Debug, Clone, Default)]
pub struct XAdd {
    pub key: Bytes,
    pub options: XAddOptions,
}

impl XAdd {
    /// Creates a new `XAdd` command instance.
    /// This constructor is primarily used internally for AOF/SPLDB reconstruction.
    pub fn new_internal(
        key: Bytes,
        id_spec: Option<StreamId>,
        fields: IndexMap<Bytes, Bytes>,
    ) -> Self {
        Self {
            key,
            options: XAddOptions {
                id_spec,
                fields,
                ..Default::default()
            },
        }
    }

    /// Helper function containing the core XADD logic, to be called after locks are acquired.
    async fn execute_with_guard<'a>(
        &self,
        shard: &Arc<DbShard>,
        guard: &mut MutexGuard<'a, ShardCache>,
    ) -> Result<(RespValue, WriteOutcome, StreamId), SpinelDBError> {
        let new_id;
        let entry = guard.get_or_insert_with_mut(self.key.clone(), || {
            let mut stream = Stream::new();
            if let Some((is_approx, count)) = self.options.maxlen {
                stream.maxlen = Some(count);
                stream.maxlen_is_approximate = is_approx;
            }
            StoredValue::new(DataValue::Stream(stream))
        });

        if let DataValue::Stream(stream) = &mut entry.data {
            let old_mem = stream.memory_usage();
            new_id = stream
                .add_entry(self.options.id_spec, self.options.fields.clone())
                .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?;

            stream.trim();

            let new_mem = stream.memory_usage();
            entry.size = new_mem;
            if new_mem != old_mem {
                let mem_diff = new_mem as isize - old_mem as isize;
                if mem_diff > 0 {
                    shard
                        .current_memory
                        .fetch_add(mem_diff as usize, Ordering::Relaxed);
                } else {
                    shard
                        .current_memory
                        .fetch_sub(mem_diff.unsigned_abs(), Ordering::Relaxed);
                }
            }
            entry.version += 1;
        } else {
            return Err(SpinelDBError::WrongType);
        }

        Ok((
            RespValue::BulkString(new_id.to_string().into()),
            WriteOutcome::Write { keys_modified: 1 },
            new_id,
        ))
    }
}

impl ParseCommand for XAdd {
    /// Parses the `XADD` command's arguments from a slice of `RespFrame`.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("XADD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let mut options = XAddOptions::default();
        let mut i = 1;

        // Parse optional arguments like NOMKSTREAM and MAXLEN.
        while i < args.len() {
            let Ok(arg_str) = extract_string(&args[i]) else {
                // Not a string, must be the start of the ID argument.
                break;
            };
            match arg_str.to_ascii_lowercase().as_str() {
                "nomkstream" => {
                    options.nomkstream = true;
                    i += 1;
                }
                "maxlen" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let approx_str = extract_string(&args[i])?;
                    let is_approx = if approx_str == "~" {
                        i += 1;
                        true
                    } else {
                        false
                    };
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let count = extract_string(&args[i])?.parse()?;
                    options.maxlen = Some((is_approx, count));
                    i += 1;
                }
                _ => break, // Reached the ID argument.
            }
        }

        // Parse the mandatory ID argument.
        if i >= args.len() {
            return Err(SpinelDBError::WrongArgumentCount("XADD".to_string()));
        }
        let id_str = extract_string(&args[i])?;
        options.id_spec = if id_str == "*" {
            None
        } else {
            Some(
                id_str
                    .parse::<StreamId>()
                    .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?,
            )
        };
        i += 1;

        // Parse the mandatory field-value pairs.
        if (args.len() - i) % 2 != 0 || (args.len() - i) == 0 {
            return Err(SpinelDBError::WrongArgumentCount("XADD".to_string()));
        }
        options.fields = args[i..]
            .chunks_exact(2)
            .map(|chunk| Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?)))
            .collect::<Result<_, SpinelDBError>>()?;

        Ok(XAdd { key, options })
    }
}

#[async_trait]
impl ExecutableCommand for XAdd {
    /// Executes the `XADD` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Phase 1: Check memory and evict if necessary, BEFORE taking the final lock.
        // This avoids holding locks during potentially slow eviction cycles.
        if let Some(maxmem) = ctx.state.config.lock().await.maxmemory {
            const MAX_EVICTION_ATTEMPTS: usize = 10;
            for _ in 0..MAX_EVICTION_ATTEMPTS {
                let new_entry_size: usize = self
                    .options
                    .fields
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum();
                let total_memory: usize =
                    ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();

                if total_memory.saturating_add(new_entry_size) <= maxmem {
                    break;
                }

                if !ctx.db.evict_one_key(&ctx.state).await {
                    break; // Stop trying if eviction fails to remove a key
                }
            }

            // Re-check after eviction attempts.
            let total_memory: usize = ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();
            let new_entry_size: usize = self
                .options
                .fields
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum();
            if total_memory.saturating_add(new_entry_size) > maxmem {
                return Err(SpinelDBError::MaxMemoryReached);
            }
        }

        // Phase 2: Now it's safe to take the lock and perform the write.
        let (shard, guard) = ctx.get_single_shard_context_mut()?;

        // Moved NOMKSTREAM check inside the lock to prevent race conditions.
        // This ensures the check and potential creation are atomic.
        if self.options.nomkstream && guard.peek(&self.key).is_none_or(|e| e.is_expired()) {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        let (resp, outcome, _) = self.execute_with_guard(shard, guard).await?;

        // Notify any waiting XREAD clients.
        ctx.state.stream_blocker_manager.notify(&self.key);
        Ok((resp, outcome))
    }
}

impl CommandSpec for XAdd {
    fn name(&self) -> &'static str {
        "xadd"
    }
    fn arity(&self) -> i64 {
        -5
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
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
        let mut args = vec![self.key.clone()];
        if self.options.nomkstream {
            args.push("NOMKSTREAM".into());
        }
        if let Some((approx, count)) = self.options.maxlen {
            args.push("MAXLEN".into());
            if approx {
                args.push("~".into());
            }
            args.push(count.to_string().into());
        }
        args.push(
            self.options
                .id_spec
                .map_or("*".to_string(), |id| id.to_string())
                .into(),
        );
        args.extend(
            self.options
                .fields
                .iter()
                .flat_map(|(k, v)| vec![k.clone(), v.clone()]),
        );
        args
    }
}
