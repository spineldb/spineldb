// src/core/commands/list/blpop.rs

//! Implements the `BLPOP` command.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::{ExecutionContext, PopDirection};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

/// Represents the `BLPOP` command with its parsed arguments.
#[derive(Debug, Clone, Default)]
pub struct BLPop {
    pub keys: Vec<Bytes>,
    pub timeout: Duration,
}

impl ParseCommand for BLPop {
    /// Parses the `BLPOP` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("BLPOP".to_string()));
        }

        let timeout_str = match args.last().unwrap() {
            RespFrame::BulkString(bs) => String::from_utf8_lossy(bs),
            _ => return Err(SpinelDBError::NotAnInteger),
        };
        let timeout_secs: f64 = timeout_str
            .parse()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        let timeout_duration = if timeout_secs <= 0.0 {
            Duration::from_secs(u64::MAX)
        } else {
            Duration::from_secs_f64(timeout_secs)
        };

        Ok(BLPop {
            keys,
            timeout: timeout_duration,
        })
    }
}

#[async_trait]
impl ExecutableCommand for BLPop {
    /// Executes the `BLPOP` command.
    /// The complex blocking logic, including race condition prevention, is
    /// delegated to the central `BlockerManager`.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state = ctx.state.clone();
        state
            .blocker_manager
            .orchestrate_blocking_pop(ctx, &self.keys, PopDirection::Left, self.timeout)
            .await
    }
}

impl CommandSpec for BLPop {
    fn name(&self) -> &'static str {
        "blpop"
    }

    fn arity(&self) -> i64 {
        -3
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::NO_PROPAGATE | CommandFlags::MOVABLEKEYS
    }

    fn first_key(&self) -> i64 {
        1
    }

    fn last_key(&self) -> i64 {
        -2
    }

    fn step(&self) -> i64 {
        1
    }

    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = self.keys.clone();
        args.push(self.timeout.as_secs_f64().to_string().into());
        args
    }
}
