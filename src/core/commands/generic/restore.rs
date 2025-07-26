// src/core/commands/generic/restore.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};

use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct Restore {
    pub key: Bytes,
    pub ttl_ms: u64,
    pub serialized_value: Bytes,
    pub replace: bool,
}

impl ParseCommand for Restore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("RESTORE".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let ttl_ms = extract_string(&args[1])?.parse()?;
        let serialized_value = extract_bytes(&args[2])?;

        let mut replace = false;
        if args.len() > 3 {
            if extract_string(&args[3])?.eq_ignore_ascii_case("replace") {
                replace = true;
            } else {
                return Err(SpinelDBError::SyntaxError);
            }
        }

        Ok(Restore {
            key,
            ttl_ms,
            serialized_value,
            replace,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Restore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        if !self.replace && guard.peek(&self.key).is_some_and(|e| !e.is_expired()) {
            return Err(SpinelDBError::InvalidState(
                "BUSYKEY Target key name already exists.".to_string(),
            ));
        }

        // Use the SPLDB parser to convert bytes back into a StoredValue
        let mut value_to_restore =
            crate::core::persistence::spldb::deserialize_value(&self.serialized_value)?;

        // Set TTL if provided
        if self.ttl_ms > 0 {
            value_to_restore.expiry = Some(Instant::now() + Duration::from_millis(self.ttl_ms));
        }

        guard.put(self.key.clone(), value_to_restore);

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for Restore {
    fn name(&self) -> &'static str {
        "restore"
    }
    fn arity(&self) -> i64 {
        -4
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
        let mut args = vec![
            self.key.clone(),
            self.ttl_ms.to_string().into(),
            self.serialized_value.clone(),
        ];
        if self.replace {
            args.push("REPLACE".into());
        }
        args
    }
}
