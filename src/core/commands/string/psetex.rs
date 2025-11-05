// src/core/commands/string/psetex.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::commands::string::set::{Set, SetCondition, TtlOption};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct PSetEx {
    pub key: Bytes,
    pub milliseconds: u64,
    pub value: Bytes,
}

impl ParseCommand for PSetEx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "PSETEX")?;
        let milliseconds = extract_string(&args[1])?.parse::<u64>()?;
        if milliseconds == 0 {
            return Err(SpinelDBError::InvalidState(
                "invalid expire time in PSETEX".into(),
            ));
        }
        Ok(PSetEx {
            key: extract_bytes(&args[0])?,
            milliseconds,
            value: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for PSetEx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Delegasikan eksekusi ke perintah Set yang lebih generik.
        let set_cmd = Set {
            key: self.key.clone(),
            value: self.value.clone(),
            ttl: TtlOption::Milliseconds(self.milliseconds),
            condition: SetCondition::None,
            get: false,
        };
        set_cmd.execute(ctx).await
    }
}

impl CommandSpec for PSetEx {
    fn name(&self) -> &'static str {
        "psetex"
    }
    fn arity(&self) -> i64 {
        4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            self.key.clone(),
            self.milliseconds.to_string().into(),
            self.value.clone(),
        ]
    }
}
