// src/core/commands/command_spec.rs

//! Defines the `CommandSpec` trait, which provides metadata about a command.
//! This is used for introspection (e.g., the `COMMAND` command) and can also be
//! used by the router for more advanced dispatch logic in the future.

use crate::core::commands::command_trait::CommandFlags;
use bytes::Bytes;

/// A trait for describing a command's properties, such as its name, arity, flags,
/// and how to extract keys from its arguments.
pub trait CommandSpec {
    /// The name of the command in lowercase.
    fn name(&self) -> &'static str;

    /// The arity of the command.
    /// - Positive integer: fixed number of arguments.
    /// - Negative integer: minimum number of arguments (e.g., -2 for `GET key`).
    fn arity(&self) -> i64;

    /// A bitmask of flags describing the command's behavior (e.g., `WRITE`, `READONLY`).
    fn flags(&self) -> CommandFlags;

    /// The position of the first key argument. (1-based index)
    fn first_key(&self) -> i64;

    /// The position of the last key argument. (1-based, can be -1 for "all remaining").
    fn last_key(&self) -> i64;

    /// The step count between key arguments.
    fn step(&self) -> i64;

    /// Extracts the key(s) from a parsed command instance.
    fn get_keys(&self) -> Vec<Bytes>;

    /// Converts the parsed command's arguments back into a vector of `Bytes`
    /// for serialization (used for replication/AOF).
    fn to_resp_args(&self) -> Vec<Bytes>;
}
