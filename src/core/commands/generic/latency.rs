// [BARU] src/core/commands/generic/latency.rs
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub enum LatencySubcommand {
    #[default]
    Doctor,
    History(String),
}

#[derive(Debug, Clone, Default)]
pub struct Latency {
    pub subcommand: LatencySubcommand,
}

impl ParseCommand for Latency {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("LATENCY".to_string()));
        }
        let sub_str = match &args[0] {
            RespFrame::BulkString(bs) => String::from_utf8(bs.to_vec())
                .map_err(|_| SpinelDBError::WrongType)?
                .to_ascii_lowercase(),
            _ => return Err(SpinelDBError::WrongType),
        };
        let subcommand = match sub_str.as_str() {
            "doctor" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "LATENCY DOCTOR".to_string(),
                    ));
                }
                LatencySubcommand::Doctor
            }
            "history" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "LATENCY HISTORY".to_string(),
                    ));
                }
                LatencySubcommand::History(extract_string(&args[1])?)
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("LATENCY {sub_str}"))),
        };
        Ok(Latency { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Latency {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let response = match &self.subcommand {
            LatencySubcommand::Doctor => {
                let report = ctx.state.latency_monitor.get_doctor_report();
                RespValue::BulkString(report.into())
            }
            LatencySubcommand::History(event) => ctx.state.latency_monitor.get_history(event)?,
        };
        Ok((response, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Latency {
    fn name(&self) -> &'static str {
        "latency"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE | CommandFlags::READONLY
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
        match &self.subcommand {
            LatencySubcommand::Doctor => vec!["DOCTOR".into()],
            LatencySubcommand::History(event) => vec!["HISTORY".into(), event.clone().into()],
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
        assert!(Latency::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_doctor() {
        let cmd = Latency::parse(&[bulk("DOCTOR")]).unwrap();
        assert!(matches!(cmd.subcommand, LatencySubcommand::Doctor));
    }

    #[test]
    fn test_parse_doctor_case_insensitive() {
        let cmd = Latency::parse(&[bulk("doctor")]).unwrap();
        assert!(matches!(cmd.subcommand, LatencySubcommand::Doctor));
    }

    #[test]
    fn test_parse_doctor_extra_args_errors() {
        assert!(Latency::parse(&[bulk("DOCTOR"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_history() {
        let cmd = Latency::parse(&[bulk("HISTORY"), bulk("command")]).unwrap();
        match &cmd.subcommand {
            LatencySubcommand::History(e) => assert_eq!(e, "command"),
            LatencySubcommand::Doctor => panic!("expected History"),
        }
    }

    #[test]
    fn test_parse_history_no_event_errors() {
        assert!(Latency::parse(&[bulk("HISTORY")]).is_err());
    }

    #[test]
    fn test_parse_history_extra_args_errors() {
        assert!(Latency::parse(&[bulk("HISTORY"), bulk("event"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_unknown_subcommand_errors() {
        assert!(Latency::parse(&[bulk("UNKNOWN")]).is_err());
    }

    #[test]
    fn test_parse_non_bulk_string_errors() {
        assert!(Latency::parse(&[RespFrame::Integer(42)]).is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Latency::default().name(), "latency");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Latency::default().arity(), -2);
    }

    #[test]
    fn test_command_flags() {
        let flags = Latency::default().flags();
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
        assert!(flags.contains(CommandFlags::READONLY));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Latency::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Latency::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args_doctor() {
        let cmd = Latency {
            subcommand: LatencySubcommand::Doctor,
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"DOCTOR")]);
    }

    #[test]
    fn test_to_resp_args_history() {
        let cmd = Latency {
            subcommand: LatencySubcommand::History("command".into()),
        };
        assert_eq!(
            cmd.to_resp_args(),
            vec![
                Bytes::from_static(b"HISTORY"),
                Bytes::from_static(b"command")
            ]
        );
    }
}
