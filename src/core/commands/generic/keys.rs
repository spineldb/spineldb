// src/core/commands/generic/keys.rs

//! Implements the `KEYS` command.
//!
//! # WARNING: Production Use
//!
//! `KEYS` is a command that should be used with extreme caution in production
//! environments. It performs a linear scan of the entire keyspace of the
//! current database, which can be very slow and CPU-intensive on databases
//! with a large number of keys.
//!
//! The current implementation locks **all database shards** for the duration of
//! the command to ensure a consistent snapshot. This will block all other
//! commands on the same database, including writes, until the `KEYS` operation is complete,
//! potentially causing high latency for other clients.
//!
//! **For production applications, always prefer the `SCAN` command**, which
//! iterates through the keyspace incrementally without blocking the server for
//! extended periods.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::commands::scan::glob_match;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Keys {
    pub pattern: Bytes,
}

impl ParseCommand for Keys {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "KEYS")?;
        Ok(Keys {
            pattern: extract_bytes(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Keys {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if let ExecutionLocks::All { guards } = &ctx.locks {
            let mut matched_keys = Vec::new();
            for guard in guards.iter() {
                for (key, value) in guard.iter() {
                    // Check for expiration and match the glob pattern.
                    if !value.is_expired() && glob_match(&self.pattern, key) {
                        matched_keys.push(RespValue::BulkString(key.clone()));
                    }
                }
            }
            Ok((RespValue::Array(matched_keys), WriteOutcome::DidNotWrite))
        } else {
            // This is an invariant check; the router should always provide all locks for KEYS.
            Err(SpinelDBError::Internal(
                "KEYS requires all shard locks for the current database".into(),
            ))
        }
    }
}

impl CommandSpec for Keys {
    fn name(&self) -> &'static str {
        "keys"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }
    fn first_key(&self) -> i64 {
        0
    }
    fn last_key(&self) -> i64 {
        0
    }
    fn step(&self) -> i64 {
        0
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.pattern.clone()]
    }
}
