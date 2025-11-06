// src/core/commands/generic/memory.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum MemorySubcommand {
    Usage(Bytes),
}

#[derive(Debug, Clone, Default)]
pub struct Memory {
    pub subcommand: MemorySubcommand,
}

impl Default for MemorySubcommand {
    fn default() -> Self {
        MemorySubcommand::Usage(Bytes::new())
    }
}

impl ParseCommand for Memory {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("MEMORY".to_string()));
        }
        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "usage" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "MEMORY USAGE".to_string(),
                    ));
                }
                MemorySubcommand::Usage(extract_bytes(&args[1])?)
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("MEMORY {sub_str}"))),
        };
        Ok(Memory { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Memory {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let response = match &self.subcommand {
            MemorySubcommand::Usage(key) => {
                let (_, guard) = ctx.get_single_shard_context_mut()?;
                if let Some(entry) = guard.peek(key) {
                    if entry.is_expired() {
                        RespValue::Null
                    } else {
                        RespValue::Integer(entry.memory_usage() as i64)
                    }
                } else {
                    RespValue::Null
                }
            }
        };
        Ok((response, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Memory {
    fn name(&self) -> &'static str {
        "memory"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        match self.subcommand {
            MemorySubcommand::Usage(_) => 2,
        }
    }
    fn last_key(&self) -> i64 {
        match self.subcommand {
            MemorySubcommand::Usage(_) => 2,
        }
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        match &self.subcommand {
            MemorySubcommand::Usage(key) => vec![key.clone()],
        }
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        match &self.subcommand {
            MemorySubcommand::Usage(key) => vec!["USAGE".into(), key.clone()],
        }
    }
}
