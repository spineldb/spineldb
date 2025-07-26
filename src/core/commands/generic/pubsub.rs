// src/core/commands/generic/pubsub.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::commands::scan::glob_match;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum PubSubSubcommand {
    Channels(Option<Bytes>),
    NumSub(Vec<Bytes>),
    NumPat,
}

impl Default for PubSubSubcommand {
    fn default() -> Self {
        PubSubSubcommand::Channels(None)
    }
}

/// A command for introspecting the Pub/Sub system.
/// Corresponds to the `PUBSUB` SpinelDB command.
#[derive(Debug, Clone, Default)]
pub struct PubSubInfo {
    pub subcommand: PubSubSubcommand,
}

impl ParseCommand for PubSubInfo {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("PUBSUB".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "channels" => {
                let pattern = if args.len() > 1 {
                    Some(extract_bytes(&args[1])?)
                } else {
                    None
                };
                PubSubSubcommand::Channels(pattern)
            }
            "numsub" => {
                let channels = args[1..]
                    .iter()
                    .map(extract_bytes)
                    .collect::<Result<_, _>>()?;
                PubSubSubcommand::NumSub(channels)
            }
            "numpat" => {
                if args.len() > 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "PUBSUB NUMPAT".to_string(),
                    ));
                }
                PubSubSubcommand::NumPat
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("PUBSUB {sub_str}"))),
        };

        Ok(PubSubInfo { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for PubSubInfo {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let pubsub = &ctx.state.pubsub;
        match &self.subcommand {
            PubSubSubcommand::Channels(pattern) => {
                let channels: Vec<RespValue> = pubsub
                    .get_all_channels()
                    .into_iter()
                    .filter(|channel_name| match pattern {
                        Some(p) => glob_match(p, channel_name),
                        None => true,
                    })
                    .map(RespValue::BulkString)
                    .collect();
                Ok((RespValue::Array(channels), WriteOutcome::DidNotWrite))
            }
            PubSubSubcommand::NumSub(channels) => {
                let mut result = Vec::with_capacity(channels.len() * 2);
                for channel_name in channels {
                    let count = pubsub.get_subscriber_count(channel_name);
                    result.push(RespValue::BulkString(channel_name.clone()));
                    result.push(RespValue::Integer(count as i64));
                }
                Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
            }
            PubSubSubcommand::NumPat => {
                let count = pubsub.get_pattern_subscriber_count();
                Ok((RespValue::Integer(count as i64), WriteOutcome::DidNotWrite))
            }
        }
    }
}

impl CommandSpec for PubSubInfo {
    fn name(&self) -> &'static str {
        "pubsub"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::PUBSUB | CommandFlags::NO_PROPAGATE | CommandFlags::READONLY
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
        // This is a simplified representation for logging/AOF.
        match &self.subcommand {
            PubSubSubcommand::Channels(_) => vec!["CHANNELS".into()],
            PubSubSubcommand::NumSub(_) => vec!["NUMSUB".into()],
            PubSubSubcommand::NumPat => vec!["NUMPAT".into()],
        }
    }
}
