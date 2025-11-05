// src/core/commands/list/blmove.rs

//! Implements the `BLMOVE` command, a blocking version of `LMOVE`.

use super::lmove::Side;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

/// The parsed `BLMOVE` command with its arguments.
#[derive(Debug, Clone, Default)]
pub struct BLMove {
    pub source: Bytes,
    pub destination: Bytes,
    pub from: Side,
    pub to: Side,
    pub timeout: Duration,
}

impl ParseCommand for BLMove {
    /// Parses the `BLMOVE` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 5, "BLMOVE")?;
        let source = extract_bytes(&args[0])?;
        let destination = extract_bytes(&args[1])?;
        let from = match extract_string(&args[2])?.to_ascii_lowercase().as_str() {
            "left" => Side::Left,
            "right" => Side::Right,
            _ => return Err(SpinelDBError::SyntaxError),
        };
        let to = match extract_string(&args[3])?.to_ascii_lowercase().as_str() {
            "left" => Side::Left,
            "right" => Side::Right,
            _ => return Err(SpinelDBError::SyntaxError),
        };

        let timeout_secs: f64 = extract_string(&args[4])?
            .parse()
            .map_err(|_| SpinelDBError::NotAFloat)?;

        // A timeout of 0 means block indefinitely.
        let timeout_duration = if timeout_secs <= 0.0 {
            Duration::from_secs(u64::MAX) // Effectively infinite
        } else {
            Duration::from_secs_f64(timeout_secs)
        };

        Ok(BLMove {
            source,
            destination,
            from,
            to,
            timeout: timeout_duration,
        })
    }
}

#[async_trait]
impl ExecutableCommand for BLMove {
    /// Executes the `BLMOVE` command.
    /// The complex blocking logic, including race condition prevention, is
    /// delegated to the central `BlockerManager`.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state = ctx.state.clone();
        state
            .blocker_manager
            .orchestrate_blmove(
                ctx,
                &self.source,
                &self.destination,
                self.from,
                self.to,
                self.timeout,
            )
            .await
    }
}

impl CommandSpec for BLMove {
    fn name(&self) -> &'static str {
        "blmove"
    }
    fn arity(&self) -> i64 {
        6
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
            | CommandFlags::DENY_OOM
            | CommandFlags::NO_PROPAGATE
            | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            self.source.clone(),
            self.destination.clone(),
            (if self.from == Side::Left {
                "LEFT"
            } else {
                "RIGHT"
            })
            .into(),
            (if self.to == Side::Left {
                "LEFT"
            } else {
                "RIGHT"
            })
            .into(),
            self.timeout.as_secs_f64().to_string().into(),
        ]
    }
}
