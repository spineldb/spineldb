// src/core/commands/generic/backup.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::persistence::spldb;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use tracing::info;

#[derive(Debug, Clone, Default)]
pub struct Backup {
    pub path: String,
}

impl ParseCommand for Backup {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "BACKUP")?;
        Ok(Backup {
            path: extract_string(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Backup {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state_clone = ctx.state.clone();
        let path_clone = self.path.clone();

        // Spawn in a blocking task to avoid blocking the main runtime.
        let save_result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async { spldb::save(&state_clone.dbs, &path_clone).await })
        })
        .await;

        match save_result {
            Ok(Ok(_)) => {
                info!("Manual backup to '{}' completed successfully.", self.path);
                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            Ok(Err(e)) => Err(SpinelDBError::Internal(format!(
                "Failed to save backup to '{}': {}",
                self.path, e
            ))),

            Err(join_err) => Err(SpinelDBError::Internal(format!(
                "Backup task panicked: {join_err}"
            ))),
        }
    }
}

impl CommandSpec for Backup {
    fn name(&self) -> &'static str {
        "backup"
    }
    fn arity(&self) -> i64 {
        2
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
        vec![self.path.clone().into()]
    }
}
