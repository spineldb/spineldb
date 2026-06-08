// src/core/commands/generic/slowlog.rs

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
pub enum SlowlogSubcommand {
    Get(Option<usize>),
    #[default]
    Len,
    Reset,
}

#[derive(Debug, Clone, Default)]
pub struct Slowlog {
    subcommand: SlowlogSubcommand,
}

impl ParseCommand for Slowlog {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SLOWLOG".to_string()));
        }
        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "get" => {
                if args.len() > 2 {
                    return Err(SpinelDBError::WrongArgumentCount("SLOWLOG GET".to_string()));
                }
                let count = if args.len() == 2 {
                    Some(extract_string(&args[1])?.parse()?)
                } else {
                    None
                };
                SlowlogSubcommand::Get(count)
            }
            "len" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount("SLOWLOG LEN".to_string()));
                }
                SlowlogSubcommand::Len
            }
            "reset" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "SLOWLOG RESET".to_string(),
                    ));
                }
                SlowlogSubcommand::Reset
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("SLOWLOG {sub_str}"))),
        };
        Ok(Slowlog { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Slowlog {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let response = match self.subcommand {
            SlowlogSubcommand::Get(count) => ctx.state.latency_monitor.get_slow_log(count),
            SlowlogSubcommand::Len => ctx.state.latency_monitor.get_slow_log_len(),
            SlowlogSubcommand::Reset => ctx.state.latency_monitor.reset_slow_log(),
        };
        Ok((response, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Slowlog {
    fn name(&self) -> &'static str {
        "slowlog"
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
        let mut args = vec![];
        match self.subcommand {
            SlowlogSubcommand::Get(Some(c)) => {
                args.extend(["GET".into(), c.to_string().into()]);
            }
            SlowlogSubcommand::Get(None) => args.push("GET".into()),
            SlowlogSubcommand::Len => args.push("LEN".into()),
            SlowlogSubcommand::Reset => args.push("RESET".into()),
        }
        args
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
        assert!(Slowlog::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_get_no_args() {
        let cmd = Slowlog::parse(&[bulk("GET")]).unwrap();
        assert!(matches!(cmd.subcommand, SlowlogSubcommand::Get(None)));
    }

    #[test]
    fn test_parse_get_with_count() {
        let cmd = Slowlog::parse(&[bulk("GET"), bulk("5")]).unwrap();
        assert!(matches!(cmd.subcommand, SlowlogSubcommand::Get(Some(5))));
    }

    #[test]
    fn test_parse_get_case_insensitive() {
        let cmd = Slowlog::parse(&[bulk("get")]).unwrap();
        assert!(matches!(cmd.subcommand, SlowlogSubcommand::Get(None)));
    }

    #[test]
    fn test_parse_get_too_many_args_errors() {
        assert!(Slowlog::parse(&[bulk("GET"), bulk("5"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_get_bad_count_errors() {
        assert!(Slowlog::parse(&[bulk("GET"), bulk("abc")]).is_err());
    }

    #[test]
    fn test_parse_len() {
        let cmd = Slowlog::parse(&[bulk("LEN")]).unwrap();
        assert!(matches!(cmd.subcommand, SlowlogSubcommand::Len));
    }

    #[test]
    fn test_parse_len_extra_args_errors() {
        assert!(Slowlog::parse(&[bulk("LEN"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_reset() {
        let cmd = Slowlog::parse(&[bulk("RESET")]).unwrap();
        assert!(matches!(cmd.subcommand, SlowlogSubcommand::Reset));
    }

    #[test]
    fn test_parse_reset_extra_args_errors() {
        assert!(Slowlog::parse(&[bulk("RESET"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_unknown_subcommand_errors() {
        let result = Slowlog::parse(&[bulk("UNKNOWN")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Slowlog::default().name(), "slowlog");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Slowlog::default().arity(), -2);
    }

    #[test]
    fn test_command_flags() {
        let flags = Slowlog::default().flags();
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
        assert!(flags.contains(CommandFlags::READONLY));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Slowlog::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Slowlog::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args_get_none() {
        let cmd = Slowlog {
            subcommand: SlowlogSubcommand::Get(None),
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"GET")]);
    }

    #[test]
    fn test_to_resp_args_get_with_count() {
        let cmd = Slowlog {
            subcommand: SlowlogSubcommand::Get(Some(10)),
        };
        assert_eq!(
            cmd.to_resp_args(),
            vec![Bytes::from_static(b"GET"), Bytes::from_static(b"10")]
        );
    }

    #[test]
    fn test_to_resp_args_len() {
        let cmd = Slowlog {
            subcommand: SlowlogSubcommand::Len,
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"LEN")]);
    }

    #[test]
    fn test_to_resp_args_reset() {
        let cmd = Slowlog {
            subcommand: SlowlogSubcommand::Reset,
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"RESET")]);
    }
}
