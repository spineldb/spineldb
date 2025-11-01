// src/core/commands/search/mod.rs

pub mod aggregate;
pub mod create;
pub mod drop;
pub mod exec;
pub mod info;
pub mod profile;
pub mod spellcheck;
pub mod suggest;
pub mod synonym;
pub mod synonym_dump;

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

pub use aggregate::FtAggregateCommand;
pub use create::FtCreateCommand;
pub use drop::FtDropCommand;
pub use exec::FtSearchCommand;
pub use info::FtInfoCommand;
pub use profile::FtProfileCommand;
pub use spellcheck::FtSpellCheckCommand;
pub use suggest::FtSuggestCommand;
pub use synonym::FtSynonymCommand;
pub use synonym_dump::FtSynonymDumpCommand;

#[derive(Debug, Clone)]
pub enum Ft {
    Aggregate(FtAggregateCommand),
    Create(FtCreateCommand),
    Drop(FtDropCommand),
    Info(FtInfoCommand),
    Search(FtSearchCommand),
    SpellCheck(FtSpellCheckCommand),
    Suggest(FtSuggestCommand),
    Synonym(FtSynonymCommand),
    Profile(FtProfileCommand),
    SynonymDump(FtSynonymDumpCommand),
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
            "aggregate" => Ok(Ft::Aggregate(FtAggregateCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
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
            "spellcheck" => Ok(Ft::SpellCheck(FtSpellCheckCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
            "suggest" => Ok(Ft::Suggest(FtSuggestCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
            "synupdate" => Ok(Ft::Synonym(FtSynonymCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
            "profile" => Ok(Ft::Profile(FtProfileCommand::parse(
                &command_args_as_spinel_strings,
            )?)),
            "syndump" => Ok(Ft::SynonymDump(FtSynonymDumpCommand::parse(
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
            Ft::Aggregate(cmd) => cmd.execute(ctx).await,
            Ft::Create(cmd) => cmd.execute(ctx).await,
            Ft::Drop(cmd) => cmd.execute(ctx).await,
            Ft::Info(cmd) => cmd.execute(ctx).await,
            Ft::Search(cmd) => cmd.execute(ctx).await,
            Ft::SpellCheck(cmd) => cmd.execute(ctx).await,
            Ft::Suggest(cmd) => cmd.execute(ctx).await,
            Ft::Synonym(cmd) => cmd.execute(ctx).await,
            Ft::SynonymDump(cmd) => cmd.execute(ctx).await,
            Ft::Profile(cmd) => cmd.execute(ctx).await,
        }
    }
}

impl CommandSpec for Ft {
    fn name(&self) -> &'static str {
        match self {
            Ft::Aggregate(cmd) => cmd.name(),
            Ft::Create(cmd) => cmd.name(),
            Ft::Drop(cmd) => cmd.name(),
            Ft::Info(cmd) => cmd.name(),
            Ft::Search(cmd) => cmd.name(),
            Ft::SpellCheck(cmd) => cmd.name(),
            Ft::Suggest(cmd) => cmd.name(),
            Ft::Synonym(cmd) => cmd.name(),
            Ft::SynonymDump(cmd) => cmd.name(),
            Ft::Profile(cmd) => cmd.name(),
        }
    }

    fn arity(&self) -> i64 {
        match self {
            Ft::Aggregate(cmd) => cmd.arity(),
            Ft::Create(cmd) => cmd.arity(),
            Ft::Drop(cmd) => cmd.arity(),
            Ft::Info(cmd) => cmd.arity(),
            Ft::Search(cmd) => cmd.arity(),
            Ft::SpellCheck(cmd) => cmd.arity(),
            Ft::Suggest(cmd) => cmd.arity(),
            Ft::Synonym(cmd) => cmd.arity(),
            Ft::SynonymDump(cmd) => cmd.arity(),
            Ft::Profile(cmd) => cmd.arity(),
        }
    }

    fn flags(&self) -> CommandFlags {
        match self {
            Ft::Aggregate(cmd) => cmd.flags(),
            Ft::Create(cmd) => cmd.flags(),
            Ft::Drop(cmd) => cmd.flags(),
            Ft::Info(cmd) => cmd.flags(),
            Ft::Search(cmd) => cmd.flags(),
            Ft::SpellCheck(cmd) => cmd.flags(),
            Ft::Suggest(cmd) => cmd.flags(),
            Ft::Synonym(cmd) => cmd.flags(),
            Ft::SynonymDump(cmd) => cmd.flags(),
            Ft::Profile(cmd) => cmd.flags(),
        }
    }

    fn first_key(&self) -> i64 {
        match self {
            Ft::Aggregate(cmd) => cmd.first_key(),
            Ft::Create(cmd) => cmd.first_key(),
            Ft::Drop(cmd) => cmd.first_key(),
            Ft::Info(cmd) => cmd.first_key(),
            Ft::Search(cmd) => cmd.first_key(),
            Ft::SpellCheck(cmd) => cmd.first_key(),
            Ft::Suggest(cmd) => cmd.first_key(),
            Ft::Synonym(cmd) => cmd.first_key(),
            Ft::SynonymDump(cmd) => cmd.first_key(),
            Ft::Profile(cmd) => cmd.first_key(),
        }
    }

    fn last_key(&self) -> i64 {
        match self {
            Ft::Aggregate(cmd) => cmd.last_key(),
            Ft::Create(cmd) => cmd.last_key(),
            Ft::Drop(cmd) => cmd.last_key(),
            Ft::Info(cmd) => cmd.last_key(),
            Ft::Search(cmd) => cmd.last_key(),
            Ft::SpellCheck(cmd) => cmd.last_key(),
            Ft::Suggest(cmd) => cmd.last_key(),
            Ft::Synonym(cmd) => cmd.last_key(),
            Ft::SynonymDump(cmd) => cmd.last_key(),
            Ft::Profile(cmd) => cmd.last_key(),
        }
    }

    fn step(&self) -> i64 {
        match self {
            Ft::Aggregate(cmd) => cmd.step(),
            Ft::Create(cmd) => cmd.step(),
            Ft::Drop(cmd) => cmd.step(),
            Ft::Info(cmd) => cmd.step(),
            Ft::Search(cmd) => cmd.step(),
            Ft::SpellCheck(cmd) => cmd.step(),
            Ft::Suggest(cmd) => cmd.step(),
            Ft::Synonym(cmd) => cmd.step(),
            Ft::SynonymDump(cmd) => cmd.step(),
            Ft::Profile(cmd) => cmd.step(),
        }
    }

    fn get_keys(&self) -> Vec<Bytes> {
        match self {
            Ft::Aggregate(cmd) => cmd.get_keys(),
            Ft::Create(cmd) => cmd.get_keys(),
            Ft::Drop(cmd) => cmd.get_keys(),
            Ft::Info(cmd) => cmd.get_keys(),
            Ft::Search(cmd) => cmd.get_keys(),
            Ft::SpellCheck(cmd) => cmd.get_keys(),
            Ft::Suggest(cmd) => cmd.get_keys(),
            Ft::Synonym(cmd) => cmd.get_keys(),
            Ft::SynonymDump(cmd) => cmd.get_keys(),
            Ft::Profile(cmd) => cmd.get_keys(),
        }
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![Bytes::from_static(b"FT")];
        let sub_args = match self {
            Ft::Aggregate(cmd) => cmd.to_resp_args(),
            Ft::Create(cmd) => cmd.to_resp_args(),
            Ft::Drop(cmd) => cmd.to_resp_args(),
            Ft::Info(cmd) => cmd.to_resp_args(),
            Ft::Search(cmd) => cmd.to_resp_args(),
            Ft::SpellCheck(cmd) => cmd.to_resp_args(),
            Ft::Suggest(cmd) => cmd.to_resp_args(),
            Ft::Synonym(cmd) => cmd.to_resp_args(),
            Ft::SynonymDump(cmd) => cmd.to_resp_args(),
            Ft::Profile(cmd) => cmd.to_resp_args(),
        };
        args.extend(sub_args);
        args
    }
}
