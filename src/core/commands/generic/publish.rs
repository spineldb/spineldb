// src/core/commands/generic/publish.rs

use crate::core::cluster::gossip::{GossipMessage, GossipTaskMessage, now_ms};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use tracing::warn;

#[derive(Debug, Clone, Default)]
pub struct Publish {
    pub channel: Bytes,
    pub message: Bytes,
}

impl ParseCommand for Publish {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "PUBLISH")?;
        Ok(Publish {
            channel: extract_bytes(&args[0])?,
            message: extract_bytes(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Publish {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let local_receivers_count = ctx
            .state
            .pubsub
            .publish(&self.channel, self.message.clone());

        if let Some(cluster_state) = &ctx.state.cluster {
            let gossip_msg = GossipMessage::Publish {
                sender_id: cluster_state.my_id.clone(),
                channel: self.channel.to_vec(),
                message: self.message.to_vec(),
                timestamp_ms: now_ms(),
            };

            let task_msg = GossipTaskMessage::Broadcast(gossip_msg);

            if let Err(e) = ctx.state.cluster_gossip_tx.try_send(task_msg) {
                warn!(
                    "Failed to send PUBLISH message to gossip worker, it may be busy or shut down: {}",
                    e
                );
            }
        }

        Ok((
            RespValue::Integer(local_receivers_count as i64),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl CommandSpec for Publish {
    fn name(&self) -> &'static str {
        "publish"
    }
    fn arity(&self) -> i64 {
        3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::PUBSUB | CommandFlags::NO_PROPAGATE
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
        vec![self.channel.clone(), self.message.clone()]
    }
}
