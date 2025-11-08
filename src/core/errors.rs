// src/core/errors.rs

//! Defines the primary error type for the entire application.

use std::error::Error as StdError;
use std::num::{ParseFloatError, ParseIntError};
use std::sync::Arc;
use thiserror::Error;

/// The main error enum, representing all possible failures within the server.
/// Using `thiserror` allows for clean error definitions and automatic `From` trait implementations.
#[derive(Error, Debug)]
pub enum SpinelDBError {
    #[error("IO Error: {0}")]
    Io(Arc<std::io::Error>),

    #[error("Incomplete data in stream")]
    IncompleteData,

    #[error("IO Error: {0}")]
    IoString(String),

    #[error("HTTP client error: {0}")]
    HttpClientError(String),

    #[error("Unknown command '{0}'")]
    UnknownCommand(String),

    #[error("Syntax error")]
    SyntaxError,

    #[error("Wrong number of arguments for '{0}' command")]
    WrongArgumentCount(String),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("Value is not an integer or out of range")]
    NotAnInteger,

    #[error("value is not a valid float")]
    NotAFloat,

    #[error("Increment or decrement would overflow")]
    Overflow,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Key already exists")]
    KeyExists,

    #[error("NOAUTH Authentication required")]
    AuthRequired,

    #[error("NOPERmission command not allowed")]
    NoPermission,

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Security violation: {0}")]
    SecurityViolation(String),

    #[error("WRONGPASS invalid password")]
    InvalidPassword,

    #[error("Command not allowed in the current state: {0}")]
    InvalidState(String),

    #[error("Transaction aborted (WATCH failed)")]
    TransactionAborted,

    #[error("OOM command not allowed when used memory > 'maxmemory'")]
    MaxMemoryReached,

    #[error("READONLY {0}")]
    ReadOnly(String),

    #[error("Persistence Error: {0}")]
    AofError(String),

    #[error("Replication Error: {0}")]
    ReplicationError(String),

    #[error("Locking Error: {0}")]
    LockingError(String),

    #[error("Migration Error: {0}")]
    MigrationError(String),

    #[error("Internal Server Error: {0}")]
    Internal(String),

    #[error("-NOGROUP No such consumer group")]
    ConsumerGroupNotFound,

    #[error("Could not REPLICATE: replication loop detected")]
    ReplicationLoopDetected,

    #[error("Script timed out")]
    ScriptTimeout,

    // --- Cluster-specific errors ---
    /// A redirect error indicating that a key/slot has moved to a different node.
    #[error("MOVED {slot} {addr}")]
    Moved { slot: u16, addr: String },

    /// A temporary redirect error for a slot that is currently being migrated.
    #[error("ASK {slot} {addr}")]
    Ask { slot: u16, addr: String },

    /// A multi-key command was attempted on keys in different slots.
    #[error("CROSSSLOT Keys in request don't hash to the same slot")]
    CrossSlot,

    /// An error indicating that the cluster is down or a slot is unassigned.
    #[error("CLUSTERDOWN {0}")]
    ClusterDown(String),
}

// Manual implementation of Clone because `std::io::Error` is not cloneable.
// We wrap it in an Arc to allow for cheap, shared cloning.
impl Clone for SpinelDBError {
    fn clone(&self) -> Self {
        match self {
            SpinelDBError::Io(e) => SpinelDBError::Io(Arc::clone(e)),
            SpinelDBError::IncompleteData => SpinelDBError::IncompleteData,
            SpinelDBError::IoString(s) => SpinelDBError::IoString(s.clone()),
            SpinelDBError::HttpClientError(s) => SpinelDBError::HttpClientError(s.clone()),
            SpinelDBError::UnknownCommand(s) => SpinelDBError::UnknownCommand(s.clone()),
            SpinelDBError::SyntaxError => SpinelDBError::SyntaxError,
            SpinelDBError::WrongArgumentCount(s) => SpinelDBError::WrongArgumentCount(s.clone()),
            SpinelDBError::WrongType => SpinelDBError::WrongType,
            SpinelDBError::NotAnInteger => SpinelDBError::NotAnInteger,
            SpinelDBError::NotAFloat => SpinelDBError::NotAFloat,
            SpinelDBError::Overflow => SpinelDBError::Overflow,
            SpinelDBError::KeyNotFound => SpinelDBError::KeyNotFound,
            SpinelDBError::KeyExists => SpinelDBError::KeyExists,
            SpinelDBError::AuthRequired => SpinelDBError::AuthRequired,
            SpinelDBError::NoPermission => SpinelDBError::NoPermission,
            SpinelDBError::InvalidRequest(s) => SpinelDBError::InvalidRequest(s.clone()),
            SpinelDBError::SecurityViolation(s) => SpinelDBError::SecurityViolation(s.clone()),
            SpinelDBError::InvalidPassword => SpinelDBError::InvalidPassword,
            SpinelDBError::InvalidState(s) => SpinelDBError::InvalidState(s.clone()),
            SpinelDBError::TransactionAborted => SpinelDBError::TransactionAborted,
            SpinelDBError::MaxMemoryReached => SpinelDBError::MaxMemoryReached,
            SpinelDBError::ReadOnly(s) => SpinelDBError::ReadOnly(s.clone()),
            SpinelDBError::AofError(s) => SpinelDBError::AofError(s.clone()),
            SpinelDBError::ReplicationError(s) => SpinelDBError::ReplicationError(s.clone()),
            SpinelDBError::LockingError(s) => SpinelDBError::LockingError(s.clone()),
            SpinelDBError::MigrationError(s) => SpinelDBError::MigrationError(s.clone()),
            SpinelDBError::Internal(s) => SpinelDBError::Internal(s.clone()),
            SpinelDBError::ConsumerGroupNotFound => SpinelDBError::ConsumerGroupNotFound,
            SpinelDBError::ReplicationLoopDetected => SpinelDBError::ReplicationLoopDetected,
            SpinelDBError::ScriptTimeout => SpinelDBError::ScriptTimeout,
            SpinelDBError::Moved { slot, addr } => SpinelDBError::Moved {
                slot: *slot,
                addr: addr.clone(),
            },
            SpinelDBError::Ask { slot, addr } => SpinelDBError::Ask {
                slot: *slot,
                addr: addr.clone(),
            },
            SpinelDBError::CrossSlot => SpinelDBError::CrossSlot,
            SpinelDBError::ClusterDown(s) => SpinelDBError::ClusterDown(s.clone()),
        }
    }
}

impl PartialEq for SpinelDBError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SpinelDBError::Io(e1), SpinelDBError::Io(e2)) => e1.to_string() == e2.to_string(),
            (SpinelDBError::IoString(s1), SpinelDBError::IoString(s2)) => s1 == s2,
            (SpinelDBError::HttpClientError(s1), SpinelDBError::HttpClientError(s2)) => s1 == s2,
            (SpinelDBError::UnknownCommand(s1), SpinelDBError::UnknownCommand(s2)) => s1 == s2,
            (SpinelDBError::WrongArgumentCount(s1), SpinelDBError::WrongArgumentCount(s2)) => {
                s1 == s2
            }
            (SpinelDBError::InvalidRequest(s1), SpinelDBError::InvalidRequest(s2)) => s1 == s2,
            (SpinelDBError::SecurityViolation(s1), SpinelDBError::SecurityViolation(s2)) => {
                s1 == s2
            }
            (SpinelDBError::InvalidState(s1), SpinelDBError::InvalidState(s2)) => s1 == s2,
            (SpinelDBError::ReadOnly(s1), SpinelDBError::ReadOnly(s2)) => s1 == s2,
            (SpinelDBError::AofError(s1), SpinelDBError::AofError(s2)) => s1 == s2,
            (SpinelDBError::ReplicationError(s1), SpinelDBError::ReplicationError(s2)) => s1 == s2,
            (SpinelDBError::LockingError(s1), SpinelDBError::LockingError(s2)) => s1 == s2,
            (SpinelDBError::MigrationError(s1), SpinelDBError::MigrationError(s2)) => s1 == s2,
            (SpinelDBError::Internal(s1), SpinelDBError::Internal(s2)) => s1 == s2,
            (SpinelDBError::ClusterDown(s1), SpinelDBError::ClusterDown(s2)) => s1 == s2,
            (
                SpinelDBError::Moved { slot: s1, addr: a1 },
                SpinelDBError::Moved { slot: s2, addr: a2 },
            ) => s1 == s2 && a1 == a2,
            (
                SpinelDBError::Ask { slot: s1, addr: a1 },
                SpinelDBError::Ask { slot: s2, addr: a2 },
            ) => s1 == s2 && a1 == a2,
            (SpinelDBError::ConsumerGroupNotFound, SpinelDBError::ConsumerGroupNotFound) => true,
            (SpinelDBError::ReplicationLoopDetected, SpinelDBError::ReplicationLoopDetected) => {
                true
            }
            (SpinelDBError::KeyExists, SpinelDBError::KeyExists) => true,
            (SpinelDBError::TransactionAborted, SpinelDBError::TransactionAborted) => true,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

// --- From trait implementations for easy error conversion ---

impl From<std::io::Error> for SpinelDBError {
    fn from(e: std::io::Error) -> Self {
        SpinelDBError::Io(Arc::new(e))
    }
}

impl From<reqwest::Error> for SpinelDBError {
    fn from(e: reqwest::Error) -> Self {
        SpinelDBError::HttpClientError(e.to_string())
    }
}

impl From<uuid::Error> for SpinelDBError {
    fn from(e: uuid::Error) -> Self {
        SpinelDBError::Internal(format!("Failed to generate UUID: {e}"))
    }
}

impl From<std::str::Utf8Error> for SpinelDBError {
    fn from(_: std::str::Utf8Error) -> Self {
        SpinelDBError::WrongType
    }
}

impl From<std::string::FromUtf8Error> for SpinelDBError {
    fn from(_: std::string::FromUtf8Error) -> Self {
        SpinelDBError::WrongType
    }
}

impl From<String> for SpinelDBError {
    fn from(s: String) -> Self {
        SpinelDBError::IoString(s)
    }
}

impl From<ParseIntError> for SpinelDBError {
    fn from(_: ParseIntError) -> Self {
        SpinelDBError::NotAnInteger
    }
}

impl From<ParseFloatError> for SpinelDBError {
    fn from(_: ParseFloatError) -> Self {
        SpinelDBError::NotAFloat
    }
}

impl From<mlua::Error> for SpinelDBError {
    fn from(e: mlua::Error) -> Self {
        let mut source: Option<&(dyn StdError + 'static)> = e.source();
        while let Some(err) = source {
            if let Some(ignis_err) = err.downcast_ref::<SpinelDBError>() {
                return ignis_err.clone();
            }
            source = err.source();
        }
        SpinelDBError::Internal(format!("Lua error: {e}"))
    }
}

impl From<serde_json::Error> for SpinelDBError {
    fn from(e: serde_json::Error) -> Self {
        SpinelDBError::Internal(format!("JSON serialization/deserialization error: {e}"))
    }
}
