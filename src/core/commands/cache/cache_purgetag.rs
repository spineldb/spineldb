// src/core/commands/cache/cache_purgetag.rs

//! Implements the `CACHE.PURGETAG` command, which efficiently invalidates
//! all cache entries associated with one or more tags.

use crate::core::SpinelDBError;
use crate::core::cluster::gossip::{GossipMessage, GossipTaskMessage, now_ms};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::{RespFrame, RespValue};
use crate::core::state::ServerState;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, warn};

/// Represents the `CACHE.PURGETAG` command with its parsed arguments.
#[derive(Debug, Clone, Default)]
pub struct CachePurgeTag {
    pub tags: Vec<Bytes>,
}

impl ParseCommand for CachePurgeTag {
    /// Parses the command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "CACHE.PURGETAG".to_string(),
            ));
        }
        let tags = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(CachePurgeTag { tags })
    }
}

#[async_trait]
impl ExecutableCommand for CachePurgeTag {
    /// Executes the `CACHE.PURGETAG` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut deleted_count = 0;
        let mut outcome = WriteOutcome::DidNotWrite;

        // In cluster mode, generate a new epoch for this purge operation and broadcast it.
        // This ensures eventual consistency across the cluster without relying on synchronized clocks.
        if let Some(cluster_state) = &ctx.state.cluster {
            // 1. Get a new, unique epoch for this specific purge operation.
            let new_epoch = cluster_state.get_new_purge_epoch();

            // 2. Immediately update the local node's knowledge of the latest purge epoch for each tag.
            for tag in &self.tags {
                ctx.state
                    .cache
                    .tag_purge_epochs
                    .insert(tag.clone(), new_epoch);
            }

            // 3. Prepare the gossip message containing the tags and their associated purge epoch.
            let tags_for_gossip: Vec<(Vec<u8>, u64)> =
                self.tags.iter().map(|b| (b.to_vec(), new_epoch)).collect();

            let gossip_msg = GossipMessage::PurgeTags {
                sender_id: cluster_state.my_id.clone(),
                tags_with_epoch: tags_for_gossip,
                timestamp_ms: now_ms(),
            };
            let task_msg = GossipTaskMessage::Broadcast(gossip_msg);

            // 4. Send the message to the gossip task for broadcasting.
            if let Err(e) = ctx.state.cluster_gossip_tx.try_send(task_msg) {
                warn!("Failed to broadcast PURGETAG to gossip worker: {}", e);
            }
        }

        // 5. Perform the purge operation on the local node immediately for responsiveness.
        // The background validator task will handle any race conditions on other nodes.
        let local_purge_count = perform_local_purge(&ctx.state, &self.tags).await?;
        if local_purge_count > 0 {
            deleted_count = local_purge_count;
            outcome = WriteOutcome::Delete {
                keys_deleted: deleted_count as u64,
            };
        }

        Ok((RespValue::Integer(deleted_count), outcome))
    }
}

/// Performs the tag purging logic on the local node's database.
/// This function is used both by the direct command execution and by the gossip handler.
pub async fn perform_local_purge(
    state: &Arc<ServerState>,
    tags: &[Bytes],
) -> Result<i64, SpinelDBError> {
    let mut keys_to_delete = HashSet::new();
    let db = state.get_db(0).unwrap(); // Assumes cache is on DB 0

    // Phase 1: Collect all unique keys associated with the given tags.
    // We lock all shards to get a consistent view of the tag index.
    let all_guards = db.lock_all_shards().await;
    for guard in all_guards.iter() {
        for tag in tags {
            if let Some(keys_in_tag) = guard.tag_index.get(tag) {
                keys_to_delete.extend(keys_in_tag.iter().cloned());
            }
        }
    }
    drop(all_guards); // Release locks as soon as possible.

    if keys_to_delete.is_empty() {
        return Ok(0);
    }

    let keys_vec: Vec<Bytes> = keys_to_delete.into_iter().collect();
    let deleted_count = db.del(&keys_vec).await;

    debug!("CACHE.PURGETAG locally deleted {} keys.", deleted_count);

    Ok(deleted_count as i64)
}

impl CommandSpec for CachePurgeTag {
    fn name(&self) -> &'static str {
        "cache.purgetag"
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
        let mut args = vec![Bytes::from_static(b"CACHE.PURGETAG")];
        args.extend(self.tags.clone());
        args
    }
}
