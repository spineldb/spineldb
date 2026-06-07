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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_flags_combination() {
        let flags = CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS;
        assert!(flags.contains(CommandFlags::WRITE));
        assert!(flags.contains(CommandFlags::DENY_OOM));
        assert!(flags.contains(CommandFlags::MOVABLEKEYS));
        assert!(!flags.contains(CommandFlags::ADMIN));
        assert!(!flags.contains(CommandFlags::PUBSUB));
    }

    #[test]
    fn test_command_flags_empty() {
        let flags = CommandFlags::empty();
        assert!(flags.is_empty());
        assert_eq!(flags.bits(), 0);
    }

    #[test]
    fn test_command_flags_all_bits_distinct() {
        let all = [
            CommandFlags::WRITE,
            CommandFlags::READONLY,
            CommandFlags::DENY_OOM,
            CommandFlags::ADMIN,
            CommandFlags::PUBSUB,
            CommandFlags::NO_PROPAGATE,
            CommandFlags::TRANSACTION,
            CommandFlags::MOVABLEKEYS,
            CommandFlags::SCRIPTING,
        ];
        // Each flag should have exactly one bit set, and the bits should be unique.
        let mut seen = std::collections::HashSet::new();
        for f in all {
            assert_eq!(f.bits().count_ones(), 1, "flag {f:?} should have one bit");
            assert!(seen.insert(f.bits()), "bit collision for {f:?}");
        }
    }

    #[test]
    fn test_command_flags_intersection() {
        let a = CommandFlags::WRITE | CommandFlags::READONLY;
        let b = CommandFlags::WRITE | CommandFlags::DENY_OOM;
        let inter = a & b;
        assert_eq!(inter, CommandFlags::WRITE);
    }

    #[test]
    fn test_write_outcome_merge_flush_is_dominant() {
        let r = WriteOutcome::Flush.merge(WriteOutcome::Write { keys_modified: 5 });
        assert_eq!(r, WriteOutcome::Flush);
        let r = WriteOutcome::Write { keys_modified: 5 }.merge(WriteOutcome::Flush);
        assert_eq!(r, WriteOutcome::Flush);
    }

    #[test]
    fn test_write_outcome_merge_delete_delete_sums() {
        let a = WriteOutcome::Delete { keys_deleted: 3 };
        let b = WriteOutcome::Delete { keys_deleted: 2 };
        assert_eq!(a.merge(b), WriteOutcome::Delete { keys_deleted: 5 });
    }

    #[test]
    fn test_write_outcome_merge_delete_write_is_delete() {
        let a = WriteOutcome::Delete { keys_deleted: 2 };
        let b = WriteOutcome::Write { keys_modified: 4 };
        let r = a.merge(b);
        assert_eq!(r, WriteOutcome::Delete { keys_deleted: 6 });
    }

    #[test]
    fn test_write_outcome_merge_write_write_sums() {
        let a = WriteOutcome::Write { keys_modified: 2 };
        let b = WriteOutcome::Write { keys_modified: 3 };
        assert_eq!(a.merge(b), WriteOutcome::Write { keys_modified: 5 });
    }

    #[test]
    fn test_write_outcome_merge_did_not_write_is_identity_for_write() {
        let a = WriteOutcome::DidNotWrite;
        let b = WriteOutcome::Write { keys_modified: 4 };
        assert_eq!(a.merge(b), WriteOutcome::Write { keys_modified: 4 });
        assert_eq!(b.merge(a), WriteOutcome::Write { keys_modified: 4 });
    }

    #[test]
    fn test_write_outcome_merge_did_not_write_passes_through_delete() {
        let a = WriteOutcome::DidNotWrite;
        let b = WriteOutcome::Delete { keys_deleted: 7 };
        assert_eq!(a.merge(b), WriteOutcome::Delete { keys_deleted: 7 });
    }

    #[test]
    fn test_write_outcome_merge_did_not_write_with_did_not_write() {
        assert_eq!(
            WriteOutcome::DidNotWrite.merge(WriteOutcome::DidNotWrite),
            WriteOutcome::DidNotWrite
        );
    }
}
