// src/core/commands/generic/pubsub.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::commands::scan::glob_match;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::commands::command_spec::CommandSpec;
    use crate::core::commands::command_trait::ParseCommand;
    use crate::core::protocol::RespFrame;

    fn bulk(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::from(s.to_owned()))
    }

    #[test]
    fn test_parse_empty_errors() {
        assert!(PubSubInfo::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_channels_no_pattern() {
        let cmd = PubSubInfo::parse(&[bulk("CHANNELS")]).unwrap();
        assert!(matches!(cmd.subcommand, PubSubSubcommand::Channels(None)));
    }

    #[test]
    fn test_parse_channels_with_pattern() {
        let cmd = PubSubInfo::parse(&[bulk("CHANNELS"), bulk("news.*")]).unwrap();
        assert!(matches!(
            cmd.subcommand,
            PubSubSubcommand::Channels(Some(_))
        ));
    }

    #[test]
    fn test_parse_channels_case_insensitive() {
        let cmd = PubSubInfo::parse(&[bulk("channels")]).unwrap();
        assert!(matches!(cmd.subcommand, PubSubSubcommand::Channels(None)));
    }

    #[test]
    fn test_parse_numsub_no_channels() {
        let cmd = PubSubInfo::parse(&[bulk("NUMSUB")]).unwrap();
        assert!(matches!(cmd.subcommand, PubSubSubcommand::NumSub(v) if v.is_empty()));
    }

    #[test]
    fn test_parse_numsub_with_channels() {
        let cmd = PubSubInfo::parse(&[bulk("NUMSUB"), bulk("ch1"), bulk("ch2")]).unwrap();
        assert!(matches!(cmd.subcommand, PubSubSubcommand::NumSub(v) if v.len() == 2));
    }

    #[test]
    fn test_parse_numpat() {
        let cmd = PubSubInfo::parse(&[bulk("NUMPAT")]).unwrap();
        assert!(matches!(cmd.subcommand, PubSubSubcommand::NumPat));
    }

    #[test]
    fn test_parse_numpat_extra_args_errors() {
        assert!(PubSubInfo::parse(&[bulk("NUMPAT"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_unknown_subcommand_errors() {
        assert!(PubSubInfo::parse(&[bulk("UNKNOWN")]).is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(PubSubInfo::default().name(), "pubsub");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(PubSubInfo::default().arity(), -2);
    }

    #[test]
    fn test_command_flags() {
        let flags = PubSubInfo::default().flags();
        assert!(flags.contains(CommandFlags::PUBSUB));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
        assert!(flags.contains(CommandFlags::READONLY));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(PubSubInfo::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = PubSubInfo::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args_channels() {
        let cmd = PubSubInfo {
            subcommand: PubSubSubcommand::Channels(None),
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"CHANNELS")]);
    }

    #[test]
    fn test_to_resp_args_numsub() {
        let cmd = PubSubInfo {
            subcommand: PubSubSubcommand::NumSub(vec![]),
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"NUMSUB")]);
    }

    #[test]
    fn test_to_resp_args_numpat() {
        let cmd = PubSubInfo {
            subcommand: PubSubSubcommand::NumPat,
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"NUMPAT")]);
    }
}
