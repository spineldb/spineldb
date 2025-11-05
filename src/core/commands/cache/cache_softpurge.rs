// src/core/commands/cache/cache_softpurge.rs

//! Implements the `CACHE.SOFTPURGE` command.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Instant;

/// Represents the `CACHE.SOFTPURGE` command.
#[derive(Debug, Clone, Default)]
pub struct CacheSoftPurge {
    pub keys: Vec<Bytes>,
}

impl ParseCommand for CacheSoftPurge {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "CACHE.SOFTPURGE".to_string(),
            ));
        }
        let keys = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(CacheSoftPurge { keys })
    }
}

#[async_trait]
impl ExecutableCommand for CacheSoftPurge {
    /// Executes the command by marking existing cache entries as stale.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "CACHE.SOFTPURGE requires multi-key lock".into(),
                ));
            }
        };

        let mut purged_count = 0;
        for key in &self.keys {
            let shard_index = ctx.db.get_shard_index(key);
            if let Some(guard) = guards.get_mut(&shard_index)
                && let Some(entry) = guard.get_mut(key)
                && !entry.is_expired()
                && matches!(entry.data, DataValue::HttpCache { .. })
            {
                // Mark as stale by setting its expiry to now.
                // The next request will trigger SWR/Grace logic.
                entry.expiry = Some(Instant::now());
                entry.version += 1;
                purged_count += 1;
            }
        }

        let outcome = if purged_count > 0 {
            WriteOutcome::Write {
                keys_modified: purged_count,
            }
        } else {
            WriteOutcome::DidNotWrite
        };

        Ok((RespValue::Integer(purged_count as i64), outcome))
    }
}

impl CommandSpec for CacheSoftPurge {
    fn name(&self) -> &'static str {
        "cache.softpurge"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
}
