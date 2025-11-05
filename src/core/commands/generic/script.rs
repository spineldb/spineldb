// src/core/commands/script.rs

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

#[derive(Debug, Clone, Default)]
pub enum ScriptSubcommand {
    #[default]
    Flush,
    Exists(Vec<String>),
    Load(Bytes),
}

#[derive(Debug, Clone, Default)]
pub struct Script {
    pub subcommand: ScriptSubcommand,
}

impl ParseCommand for Script {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SCRIPT".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "flush" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "SCRIPT FLUSH".to_string(),
                    ));
                }
                ScriptSubcommand::Flush
            }
            "exists" => {
                if args.len() < 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "SCRIPT EXISTS".to_string(),
                    ));
                }
                let sha1s = args[1..]
                    .iter()
                    .map(extract_string)
                    .collect::<Result<_, _>>()?;
                ScriptSubcommand::Exists(sha1s)
            }
            "load" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount("SCRIPT LOAD".to_string()));
                }
                let script_body = extract_bytes(&args[1])?;
                ScriptSubcommand::Load(script_body)
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("SCRIPT {sub_str}"))),
        };

        Ok(Script { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Script {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            ScriptSubcommand::Flush => {
                ctx.state.scripting.flush();
                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::Write { keys_modified: 0 },
                ))
            }
            ScriptSubcommand::Exists(sha1s) => {
                let results = ctx.state.scripting.exists(sha1s);
                let resp_values = results.into_iter().map(RespValue::Integer).collect();
                Ok((RespValue::Array(resp_values), WriteOutcome::DidNotWrite))
            }
            ScriptSubcommand::Load(script) => {
                let sha1 = ctx.state.scripting.load(script.clone());
                Ok((
                    RespValue::BulkString(sha1.into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
        }
    }
}

impl CommandSpec for Script {
    fn name(&self) -> &'static str {
        "script"
    }

    fn arity(&self) -> i64 {
        -2
    }

    fn flags(&self) -> CommandFlags {
        match &self.subcommand {
            // FLUSH and LOAD are write operations that must be replicated.
            ScriptSubcommand::Flush | ScriptSubcommand::Load(_) => {
                CommandFlags::ADMIN | CommandFlags::WRITE
            }
            // EXISTS is a read operation and does not need to be replicated.
            ScriptSubcommand::Exists(_) => CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE,
        }
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
        match &self.subcommand {
            ScriptSubcommand::Flush => vec!["FLUSH".into()],
            ScriptSubcommand::Exists(sha1s) => {
                let mut args = vec!["EXISTS".into()];
                args.extend(sha1s.iter().map(|s| s.clone().into()));
                args
            }
            ScriptSubcommand::Load(script) => vec!["LOAD".into(), script.clone()],
        }
    }
}
