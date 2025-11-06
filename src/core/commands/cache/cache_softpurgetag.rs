// src/core/commands/cache/cache_softpurgetag.rs

//! Implements the `CACHE.SOFTPURGETAG` command.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::time::Instant;

/// Represents the `CACHE.SOFTPURGETAG` command.
#[derive(Debug, Clone, Default)]
pub struct CacheSoftPurgeTag {
    pub tags: Vec<Bytes>,
}

impl ParseCommand for CacheSoftPurgeTag {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "CACHE.SOFTPURGETAG".to_string(),
            ));
        }
        let tags = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(CacheSoftPurgeTag { tags })
    }
}

#[async_trait]
impl ExecutableCommand for CacheSoftPurgeTag {
    /// Executes the command by marking all entries with the given tags as stale.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let db = ctx.db;
        let mut keys_to_purge = HashSet::new();

        // Phase 1: Collect keys without holding locks for too long.
        let all_guards = db.lock_all_shards().await;
        for guard in all_guards.iter() {
            for tag in &self.tags {
                if let Some(keys_in_tag) = guard.tag_index.get(tag) {
                    keys_to_purge.extend(keys_in_tag.iter().cloned());
                }
            }
        }
        drop(all_guards);

        if keys_to_purge.is_empty() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let keys_vec: Vec<Bytes> = keys_to_purge.into_iter().collect();
        let mut guards = db.lock_shards_for_keys(&keys_vec).await;
        let mut purged_count = 0;

        // Phase 2: Modify the entries under lock.
        for key in keys_vec {
            let shard_index = db.get_shard_index(&key);
            if let Some(guard) = guards.get_mut(&shard_index)
                && let Some(entry) = guard.get_mut(&key)
                && !entry.is_expired()
                && matches!(entry.data, DataValue::HttpCache { .. })
            {
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

impl CommandSpec for CacheSoftPurgeTag {
    fn name(&self) -> &'static str {
        "cache.softpurgetag"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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
        self.tags.clone()
    }
}
