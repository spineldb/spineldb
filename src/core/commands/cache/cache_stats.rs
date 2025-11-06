// src/core/commands/cache/cache_stats.rs

//! Implements the `CACHE.STATS` command for observing cache performance.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::database::{ExecutionContext, NUM_SHARDS};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

/// Represents the `CACHE.STATS` command.
#[derive(Debug, Clone, Default)]
pub struct CacheStats;

impl ParseCommand for CacheStats {
    /// Parses the `CACHE.STATS` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if !args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CACHE.STATS".to_string()));
        }
        Ok(CacheStats)
    }
}

#[async_trait]
impl ExecutableCommand for CacheStats {
    /// Executes the `CACHE.STATS` command, gathering and returning cache performance metrics.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Atomically load core cache statistics.
        let hits = ctx.state.cache.hits.load(Ordering::Relaxed);
        let misses = ctx.state.cache.misses.load(Ordering::Relaxed);
        let stale_hits = ctx.state.cache.stale_hits.load(Ordering::Relaxed);
        let revalidations = ctx.state.cache.revalidations.load(Ordering::Relaxed);
        let evictions = ctx.state.cache.evictions.load(Ordering::Relaxed);

        // Calculate derived metrics for better observability.
        let total_requests = hits + misses;
        let hit_ratio = if total_requests > 0 {
            (hits as f64) / (total_requests as f64)
        } else {
            0.0
        };

        // Calculate the total number of cache variants by iterating through all shards.
        // This is a read-only operation and is acceptably fast for a stats command.
        let mut total_variants = 0;
        for db in &ctx.state.dbs {
            for shard_index in 0..NUM_SHARDS {
                let guard = db.get_shard(shard_index).entries.lock().await;
                for entry in guard.iter() {
                    if let DataValue::HttpCache { variants, .. } = &entry.1.data {
                        total_variants += variants.len();
                    }
                }
            }
        }

        // Get the number of active caching policies.
        let policies_count = ctx.state.cache.policies.read().await.len();

        // Format the hit ratio for RESP output.
        let mut buffer = ryu::Buffer::new();
        let formatted_ratio = buffer.format(hit_ratio);

        // Assemble the final response array.
        let stats = vec![
            RespValue::BulkString("hits".into()),
            RespValue::Integer(hits as i64),
            RespValue::BulkString("misses".into()),
            RespValue::Integer(misses as i64),
            RespValue::BulkString("hit_ratio".into()),
            RespValue::BulkString(Bytes::copy_from_slice(formatted_ratio.as_bytes())),
            RespValue::BulkString("stale_hits".into()),
            RespValue::Integer(stale_hits as i64),
            RespValue::BulkString("revalidations".into()),
            RespValue::Integer(revalidations as i64),
            RespValue::BulkString("evictions".into()),
            RespValue::Integer(evictions as i64),
            RespValue::BulkString("total_variants".into()),
            RespValue::Integer(total_variants as i64),
            RespValue::BulkString("policies_count".into()),
            RespValue::Integer(policies_count as i64),
        ];

        Ok((RespValue::Array(stats), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for CacheStats {
    fn name(&self) -> &'static str {
        "cache.stats"
    }
    fn arity(&self) -> i64 {
        1
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
        vec![]
    }
}
