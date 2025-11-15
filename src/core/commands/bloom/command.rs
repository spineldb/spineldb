// src/core/commands/bloom/command.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

use super::bf_add::BfAdd;
use super::bf_card::BfCard;
use super::bf_exists::BfExists;
use super::bf_info::BfInfo;
use super::bf_insert::BfInsert;
use super::bf_madd::BfMAdd;
use super::bf_mexists::BfMExists;
use super::bf_reserve::BfReserve;

/// Represents the specific Bloom filter subcommand being executed.
#[derive(Debug, Clone)]
pub enum BloomSubcommand {
    /// The `BF.RESERVE` subcommand, used to create a new Bloom filter.
    Reserve(BfReserve),
    /// The `BF.ADD` subcommand, used to add an item to a Bloom filter.
    Add(BfAdd),
    /// The `BF.MADD` subcommand, used to add multiple items to a Bloom filter.
    MAdd(BfMAdd),
    /// The `BF.EXISTS` subcommand, used to check if an item might be in a Bloom filter.
    Exists(BfExists),
    /// The `BF.MEXISTS` subcommand, used to check if multiple items might be in a Bloom filter.
    MExists(BfMExists),
    /// The `BF.INFO` subcommand, used to get information about a Bloom filter.
    Info(BfInfo),
    /// The `BF.CARD` subcommand, used to get the cardinality of a Bloom filter.
    Card(BfCard),
    /// The `BF.INSERT` subcommand, used to add items with optional creation.
    Insert(BfInsert),
}

/// Implements the top-level `BF` command, acting as a dispatcher for its subcommands.
///
/// The `BF` command itself does not perform any operation directly but delegates
/// to `BF.RESERVE`, `BF.ADD`, or `BF.EXISTS` based on the provided arguments.
#[derive(Debug, Clone, Default)]
pub struct Bloom {
    /// The specific subcommand to be executed.
    pub subcommand: Option<BloomSubcommand>,
}

impl ParseCommand for Bloom {
    /// Parses the `BF` command arguments to determine the subcommand and its arguments.
    ///
    /// The first argument after `BF` is expected to be the subcommand name (e.g., "RESERVE", "ADD", "EXISTS").
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("BF".to_string()));
        }
        let subcommand_name = match &args[0] {
            RespFrame::BulkString(bs) => String::from_utf8_lossy(bs).to_ascii_lowercase(),
            _ => return Err(SpinelDBError::SyntaxError),
        };
        let subcommand_args = &args[1..];

        let subcommand = match subcommand_name.as_str() {
            "reserve" => BloomSubcommand::Reserve(BfReserve::parse(subcommand_args)?),
            "add" => BloomSubcommand::Add(BfAdd::parse(subcommand_args)?),
            "madd" => BloomSubcommand::MAdd(BfMAdd::parse(subcommand_args)?),
            "exists" => BloomSubcommand::Exists(BfExists::parse(subcommand_args)?),
            "mexists" => BloomSubcommand::MExists(BfMExists::parse(subcommand_args)?),
            "info" => BloomSubcommand::Info(BfInfo::parse(subcommand_args)?),
            "card" => BloomSubcommand::Card(BfCard::parse(subcommand_args)?),
            "insert" => BloomSubcommand::Insert(BfInsert::parse(subcommand_args)?),
            _ => {
                return Err(SpinelDBError::UnknownCommand(format!(
                    "BF.{}",
                    subcommand_name.to_uppercase()
                )));
            }
        };

        Ok(Bloom {
            subcommand: Some(subcommand),
        })
    }
}

#[async_trait]
impl ExecutableCommand for Bloom {
    /// Executes the determined Bloom filter subcommand.
    ///
    /// This method delegates the execution to the specific subcommand's `execute` method.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            Some(BloomSubcommand::Reserve(cmd)) => cmd.execute(ctx).await,
            Some(BloomSubcommand::Add(cmd)) => cmd.execute(ctx).await,
            Some(BloomSubcommand::MAdd(cmd)) => cmd.execute(ctx).await,
            Some(BloomSubcommand::Exists(cmd)) => cmd.execute(ctx).await,
            Some(BloomSubcommand::MExists(cmd)) => cmd.execute(ctx).await,
            Some(BloomSubcommand::Info(cmd)) => cmd.execute(ctx).await,
            Some(BloomSubcommand::Card(cmd)) => cmd.execute(ctx).await,
            Some(BloomSubcommand::Insert(cmd)) => cmd.execute(ctx).await,
            None => Err(SpinelDBError::Internal("Bloom command not parsed".into())),
        }
    }
}

impl CommandSpec for Bloom {
    /// Returns the base name of the command, "bf".
    fn name(&self) -> &'static str {
        "bf"
    }
    /// Returns the arity of the command.
    ///
    /// For dispatcher commands like `BF`, arity is typically negative to indicate
    /// a variable number of arguments, as the actual arity depends on the subcommand.
    fn arity(&self) -> i64 {
        -2
    }
    /// Returns the flags for the command.
    ///
    /// The top-level `BF` command is marked as `WRITE` because some of its subcommands
    /// (like `BF.ADD` and `BF.RESERVE`) modify the dataset.
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::READONLY
    }
    /// Returns the position of the first key argument.
    ///
    /// For dispatcher commands, this is often 0, as key extraction is delegated
    /// to the specific subcommands.
    fn first_key(&self) -> i64 {
        // This is tricky for a dispatcher. We'll rely on subcommand specs.
        // Let's return 0 as a safe default, actual key extraction will be handled by the subcommand.
        0
    }
    /// Returns the position of the last key argument.
    fn last_key(&self) -> i64 {
        0
    }
    /// Returns the step count between key arguments.
    fn step(&self) -> i64 {
        0
    }
    /// Extracts the key(s) from the subcommand.
    ///
    /// Delegates to the specific subcommand's `get_keys` method.
    fn get_keys(&self) -> Vec<Bytes> {
        match &self.subcommand {
            Some(BloomSubcommand::Reserve(cmd)) => cmd.get_keys(),
            Some(BloomSubcommand::Add(cmd)) => cmd.get_keys(),
            Some(BloomSubcommand::MAdd(cmd)) => cmd.get_keys(),
            Some(BloomSubcommand::Exists(cmd)) => cmd.get_keys(),
            Some(BloomSubcommand::MExists(cmd)) => cmd.get_keys(),
            Some(BloomSubcommand::Info(cmd)) => cmd.get_keys(),
            Some(BloomSubcommand::Card(cmd)) => cmd.get_keys(),
            Some(BloomSubcommand::Insert(cmd)) => cmd.get_keys(),
            None => vec![],
        }
    }
    /// Converts the subcommand's arguments back into a vector of `Bytes`.
    ///
    /// Delegates to the specific subcommand's `to_resp_args` method.
    fn to_resp_args(&self) -> Vec<Bytes> {
        match &self.subcommand {
            Some(BloomSubcommand::Reserve(cmd)) => cmd.to_resp_args(),
            Some(BloomSubcommand::Add(cmd)) => cmd.to_resp_args(),
            Some(BloomSubcommand::MAdd(cmd)) => cmd.to_resp_args(),
            Some(BloomSubcommand::Exists(cmd)) => cmd.to_resp_args(),
            Some(BloomSubcommand::MExists(cmd)) => cmd.to_resp_args(),
            Some(BloomSubcommand::Info(cmd)) => cmd.to_resp_args(),
            Some(BloomSubcommand::Card(cmd)) => cmd.to_resp_args(),
            Some(BloomSubcommand::Insert(cmd)) => cmd.to_resp_args(),
            None => vec![],
        }
    }
}
