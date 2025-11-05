// src/core/commands/generic/flushdb.rs

use crate::core::SpinelDBError;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::validate_arg_count;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::{RespFrame, RespValue};
use async_trait::async_trait;
use bytes::Bytes;

/// Represents the `FLUSHDB` command.
#[derive(Debug, Clone, Default)]
pub struct FlushDb;

impl ParseCommand for FlushDb {
    /// Parses the arguments for the FLUSHDB command.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "FLUSHDB")?;
        Ok(FlushDb)
    }
}

#[async_trait]
impl ExecutableCommand for FlushDb {
    /// Executes the FLUSHDB command by clearing all data from the current database.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // The router provides locks on all shards for this command.
        // Iterate through the existing guards and clear each shard.
        if let ExecutionLocks::All { guards } = &mut ctx.locks {
            for guard in guards.iter_mut() {
                guard.clear();
            }
        } else {
            // This case is an invariant violation, as the router should always acquire all locks.
            return Err(SpinelDBError::Internal(
                "FLUSHDB requires all shard locks".into(),
            ));
        }

        Ok((RespValue::SimpleString("OK".into()), WriteOutcome::Flush))
    }
}

impl CommandSpec for FlushDb {
    fn name(&self) -> &'static str {
        "flushdb"
    }

    fn arity(&self) -> i64 {
        1
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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
