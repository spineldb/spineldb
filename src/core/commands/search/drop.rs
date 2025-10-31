// src/core/commands/search/drop.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct FtDropCommand {
    pub index_name: String,
}

#[async_trait]
impl ExecutableCommand for FtDropCommand {
    async fn execute<'a>(
        &self,
        ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if !ctx.state.search_indexes.contains_key(&self.index_name) {
            return Err(SpinelDBError::Internal("Index does not exist".to_string()));
        }

        ctx.state.search_indexes.remove(&self.index_name);

        Ok((
            RespValue::SimpleString("OK".to_string()),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl FtDropCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "FT.DROPINDEX".to_string(),
            ));
        }
        let index_name = args[0].string_from_bytes()?;
        Ok(Self { index_name })
    }
}

impl CommandSpec for FtDropCommand {
    fn name(&self) -> &'static str {
        "ft.drop"
    }

    fn arity(&self) -> i64 {
        2 // FT.DROP index_name
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM
    }

    fn first_key(&self) -> i64 {
        1
    }

    fn last_key(&self) -> i64 {
        1
    }

    fn step(&self) -> i64 {
        1
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![Bytes::from(self.index_name.clone())]
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            Bytes::from_static(b"DROP"),
            Bytes::from(self.index_name.clone()),
        ]
    }
}
