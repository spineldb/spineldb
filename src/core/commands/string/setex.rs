// src/core/commands/string/setex.rs

use super::set::{Set, SetCondition, TtlOption};
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

#[derive(Debug, Clone, Default)]
pub struct SetEx {
    pub key: Bytes,
    pub seconds: u64,
    pub value: Bytes,
}

impl ParseCommand for SetEx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "SETEX")?;
        let seconds = extract_string(&args[1])?.parse::<u64>()?;
        if seconds == 0 {
            return Err(SpinelDBError::InvalidState(
                "invalid expire time in SETEX".into(),
            ));
        }
        Ok(SetEx {
            key: extract_bytes(&args[0])?,
            seconds,
            value: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for SetEx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Delegasikan eksekusi ke perintah Set yang lebih generik.
        let set_cmd = Set {
            key: self.key.clone(),
            value: self.value.clone(),
            ttl: TtlOption::Seconds(self.seconds),
            condition: SetCondition::None,
            get: false,
        };
        set_cmd.execute(ctx).await
    }
}

// Implementasi CommandSpec tidak berubah.
impl CommandSpec for SetEx {
    fn name(&self) -> &'static str {
        "setex"
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
            self.seconds.to_string().into(),
            self.value.clone(),
        ]
    }
}
