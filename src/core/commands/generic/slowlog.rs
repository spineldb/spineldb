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
