// src/core/commands/command_trait.rs

//! Defines the core traits for all executable commands.

use crate::core::database::ExecutionContext;
use crate::core::handler::command_router::RouteResponse;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bitflags::bitflags;
use bytes::Bytes;

bitflags! {
    /// Flags that describe the properties and behavior of a command.
    /// These are used by the router and other subsystems to handle commands appropriately.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct CommandFlags: u32 {
        /// The command modifies the dataset.
        const WRITE          = 1 << 0;
        /// The command only reads data.
        const READONLY       = 1 << 1;
        /// The command is denied if the server is out of memory (`maxmemory` is reached).
        const DENY_OOM       = 1 << 2;
        /// An administrative command.
        const ADMIN          = 1 << 3;
        /// A command related to the Pub/Sub system.
        const PUBSUB         = 1 << 4;
        /// The command should not be propagated to replicas or the AOF file.
        const NO_PROPAGATE   = 1 << 5;
        /// A command related to transactions (e.g., `MULTI`, `EXEC`).
        const TRANSACTION    = 1 << 6;
        /// The command's keys can be moved (used for cluster hashing).
        const MOVABLEKEYS    = 1 << 7;
        /// The command is a scripting command (e.g., `EVAL`).
        const SCRIPTING      = 1 << 8;
    }
}

/// Represents the outcome of a write operation, used to determine if
/// propagation to AOF/replicas is necessary and to update the dirty key counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The command did not modify any data.
    DidNotWrite,
    /// The command modified one or more keys.
    Write { keys_modified: u64 },
    /// The command deleted one or more keys.
    Delete { keys_deleted: u64 },
    /// The command flushed the entire database (e.g., `FLUSHALL`).
    Flush,
}

impl WriteOutcome {
    /// Merges two `WriteOutcome` values, prioritizing more impactful outcomes.
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Flush, _) | (_, Self::Flush) => Self::Flush,
            (Self::Delete { keys_deleted: k1 }, Self::Delete { keys_deleted: k2 }) => {
                Self::Delete {
                    keys_deleted: k1 + k2,
                }
            }
            (Self::Delete { keys_deleted: k1 }, Self::Write { keys_modified: k2 })
            | (Self::Write { keys_modified: k2 }, Self::Delete { keys_deleted: k1 }) => {
                Self::Delete {
                    keys_deleted: k1 + k2,
                } // Treat modified as deleted for aggregation
            }
            (Self::Delete { keys_deleted }, Self::DidNotWrite)
            | (Self::DidNotWrite, Self::Delete { keys_deleted }) => Self::Delete { keys_deleted },

            (Self::Write { keys_modified: k1 }, Self::Write { keys_modified: k2 }) => Self::Write {
                keys_modified: k1 + k2,
            },
            (Self::Write { keys_modified }, Self::DidNotWrite)
            | (Self::DidNotWrite, Self::Write { keys_modified }) => Self::Write { keys_modified },

            (Self::DidNotWrite, Self::DidNotWrite) => Self::DidNotWrite,
        }
    }
}

/// A composite trait that combines all necessary traits for a command.
/// It is implemented on the main `Command` enum.
#[async_trait]
pub trait CommandExt {
    /// Returns the flags for the command.
    fn get_flags(&self) -> CommandFlags;
    /// Extracts the keys from the command's arguments.
    fn get_keys(&self) -> Vec<Bytes>;

    /// Executes the command within a given `ExecutionContext`.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError>;

    /// Executes the command and potentially returns a streaming response.
    /// The default implementation buffers the response.
    async fn execute_and_stream<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<RouteResponse, SpinelDBError> {
        // Default implementation buffers the response.
        let (val, _outcome) = self.execute(ctx).await?;
        Ok(RouteResponse::Single(val))
    }
}

/// A trait for the actual execution logic of a command.
/// Implemented by each command's struct (e.g., `Get`, `Set`).
#[async_trait]
pub trait ExecutableCommand {
    /// The core logic for the command's execution.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError>;
}

/// A trait for parsing a command's arguments from a slice of `RespFrame`.
pub trait ParseCommand: Sized {
    /// Parses the arguments and returns an instance of the command struct.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError>;
}
