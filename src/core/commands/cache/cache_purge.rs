// src/core/commands/cache/cache_purge.rs

use crate::core::cluster::gossip::{GossipMessage, GossipTaskMessage, now_ms};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct CachePurge {
    pub patterns: Vec<Bytes>,
}

impl ParseCommand for CachePurge {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CACHE.PURGE".to_string()));
        }
        let patterns = args.iter().map(extract_bytes).collect::<Result<_, _>>()?;
        Ok(CachePurge { patterns })
    }
}

#[async_trait]
impl ExecutableCommand for CachePurge {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        const PURGE_PATTERN_TTL: Duration = Duration::from_secs(300);
        let expiry = Instant::now() + PURGE_PATTERN_TTL;

        for pattern in &self.patterns {
            ctx.state
                .cache
                .purge_patterns
                .insert(pattern.clone(), expiry);
        }

        if let Some(cluster_state) = &ctx.state.cluster {
            // This command is for lazy purging; the tags here are patterns.
            // We'll treat them as tags for gossip purposes to notify other nodes
            // to also start a lazy purge.
            let new_epoch = cluster_state.get_new_purge_epoch();
            let tags_for_gossip: Vec<(Vec<u8>, u64)> = self
                .patterns
                .iter()
                .map(|b| (b.to_vec(), new_epoch))
                .collect();

            let gossip_msg = GossipMessage::PurgeTags {
                sender_id: cluster_state.my_id.clone(),
                tags_with_epoch: tags_for_gossip,
                timestamp_ms: now_ms(),
            };
            let task_msg = GossipTaskMessage::Broadcast(gossip_msg);
            let _ = ctx.state.cluster_gossip_tx.try_send(task_msg);
        }

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl CommandSpec for CachePurge {
    fn name(&self) -> &'static str {
        "cache.purge"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE
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
        self.patterns.clone()
    }
}
