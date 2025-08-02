// src/core/commands/generic/shutdown.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
#[cfg(unix)]
use std::process;

use tracing::info;

#[derive(Debug, Clone, Default)]
pub struct Shutdown;

impl ParseCommand for Shutdown {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if !args.is_empty() {
            // In the future, this could support [NOSAVE|SAVE]
            if args.len() == 1 {
                let option = extract_string(&args[0])?.to_ascii_lowercase();
                if option != "save" && option != "nosave" {
                    return Err(SpinelDBError::SyntaxError);
                }
            } else {
                return Err(SpinelDBError::WrongArgumentCount("SHUTDOWN".to_string()));
            }
        }
        Ok(Shutdown)
    }
}

#[async_trait]
impl ExecutableCommand for Shutdown {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        info!("SHUTDOWN command received. Initiating server shutdown.");
        // Send a shutdown signal.
        // The easiest way is to send a signal that the main_loop will catch.
        // On Unix-like systems:
        #[cfg(unix)]
        {
            let pid = process::id();
            // This is an unsafe block, but it's a standard way to signal a process.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }

        // On other environments, a different approach might be needed,
        // but SIGTERM is the most common way to gracefully terminate.

        // We will likely never reach this point because the process will be terminating,
        // but we return OK for completeness.
        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl CommandSpec for Shutdown {
    fn name(&self) -> &'static str {
        "shutdown"
    }
    fn arity(&self) -> i64 {
        -1
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
        vec![]
    }
}
