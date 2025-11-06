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
