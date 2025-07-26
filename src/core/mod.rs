// src/core/mod.rs

//! The central module containing the core logic and data structures of SpinelDB.

pub mod acl;
pub mod blocking;
pub mod cluster;
pub mod commands;
pub mod errors;
pub mod events;
pub mod handler;
pub mod latency;
pub mod metrics;
pub mod persistence;
pub mod protocol;
pub mod pubsub;
pub mod replication;
pub mod scripting;
pub mod state;
pub mod storage;
pub mod stream_blocking;
pub mod tasks;
pub mod warden;

pub use commands::Command;
pub use errors::SpinelDBError;
pub use protocol::RespValue;
