// src/core/commands/search/mod.rs

pub mod create;
pub mod drop;
pub mod exec;
pub mod info;

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::types::SpinelString;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

pub use create::FtCreateCommand;
pub use drop::FtDropCommand;
pub use exec::FtSearchCommand;
pub use info::FtInfoCommand;

#[derive(Debug, Clone)]
pub enum Ft {
    Create(FtCreateCommand),
    Drop(FtDropCommand),
    Info(FtInfoCommand),
    Search(FtSearchCommand),
}

impl Default for Ft {
    fn default() -> Self {
        Ft::Create(FtCreateCommand::default())
    }
}

impl ParseCommand for Ft {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("FT".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let command_args = &args[1..];
        let command_args_as_spinel_strings = command_args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<SpinelString>, SpinelDBError>>()?;

        match sub_str.as_str() {
            "create" => Ok(Ft::Create(FtCreateCommand::parse(command_args)?)),
            "drop" | "dropindex" => Ok(Ft::Drop(FtDropCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
            "info" => Ok(Ft::Info(FtInfoCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
            "search" => Ok(Ft::Search(FtSearchCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
            _ => Err(SpinelDBError::UnknownCommand(format!(
                "FT.{} subcommand not found",
                sub_str
            ))),
        }
    }
}

#[async_trait]
impl ExecutableCommand for Ft {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match self {
            Ft::Create(cmd) => cmd.execute(ctx).await,
            Ft::Drop(cmd) => cmd.execute(ctx).await,
            Ft::Info(cmd) => cmd.execute(ctx).await,
            Ft::Search(cmd) => cmd.execute(ctx).await,
        }
    }
}

impl CommandSpec for Ft {
    fn name(&self) -> &'static str {
        match self {
            Ft::Create(cmd) => cmd.name(),
            Ft::Drop(cmd) => cmd.name(),
            Ft::Info(cmd) => cmd.name(),
            Ft::Search(cmd) => cmd.name(),
        }
    }

    fn arity(&self) -> i64 {
        match self {
            Ft::Create(cmd) => cmd.arity(),
            Ft::Drop(cmd) => cmd.arity(),
            Ft::Info(cmd) => cmd.arity(),
            Ft::Search(cmd) => cmd.arity(),
        }
    }

    fn flags(&self) -> CommandFlags {
        match self {
            Ft::Create(cmd) => cmd.flags(),
            Ft::Drop(cmd) => cmd.flags(),
            Ft::Info(cmd) => cmd.flags(),
            Ft::Search(cmd) => cmd.flags(),
        }
    }

    fn first_key(&self) -> i64 {
        match self {
            Ft::Create(cmd) => cmd.first_key(),
            Ft::Drop(cmd) => cmd.first_key(),
            Ft::Info(cmd) => cmd.first_key(),
            Ft::Search(cmd) => cmd.first_key(),
        }
    }

    fn last_key(&self) -> i64 {
        match self {
            Ft::Create(cmd) => cmd.last_key(),
            Ft::Drop(cmd) => cmd.last_key(),
            Ft::Info(cmd) => cmd.last_key(),
            Ft::Search(cmd) => cmd.last_key(),
        }
    }

    fn step(&self) -> i64 {
        match self {
            Ft::Create(cmd) => cmd.step(),
            Ft::Drop(cmd) => cmd.step(),
            Ft::Info(cmd) => cmd.step(),
            Ft::Search(cmd) => cmd.step(),
        }
    }

    fn get_keys(&self) -> Vec<Bytes> {
        match self {
            Ft::Create(cmd) => cmd.get_keys(),
            Ft::Drop(cmd) => cmd.get_keys(),
            Ft::Info(cmd) => cmd.get_keys(),
            Ft::Search(cmd) => cmd.get_keys(),
        }
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![Bytes::from_static(b"FT")];
        let sub_args = match self {
            Ft::Create(cmd) => cmd.to_resp_args(),
            Ft::Drop(cmd) => cmd.to_resp_args(),
            Ft::Info(cmd) => cmd.to_resp_args(),
            Ft::Search(cmd) => cmd.to_resp_args(),
        };
        args.extend(sub_args);
        args
    }
}
