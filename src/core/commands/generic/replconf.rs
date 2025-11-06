// src/core/commands/generic/replconf.rs

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
pub struct Replconf {
    pub args: Vec<String>,
}
impl ParseCommand for Replconf {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("REPLCONF".to_string()));
        }
        let str_args = args
            .iter()
            .map(extract_string)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Replconf { args: str_args })
    }
}
#[async_trait]
impl ExecutableCommand for Replconf {
    async fn execute<'a>(
        &self,
        _ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        Err(SpinelDBError::Internal(
            "REPLCONF command should not be executed directly".into(),
        ))
    }
}
impl CommandSpec for Replconf {
    fn name(&self) -> &'static str {
        "replconf"
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
        self.args.iter().map(|s| s.clone().into()).collect()
    }
}
