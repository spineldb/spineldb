// src/core/storage/db/mod.rs

//! The core database storage engine.
//! This module defines the `Db` struct, which is a sharded, in-memory key-value store.
//! It also defines the locking mechanisms and execution context for commands.

pub mod context;
pub mod core;
pub mod eviction;
pub mod locking;
pub mod shard;
pub mod transaction;
pub mod zset;

pub use self::core::{Db, NUM_SHARDS, PopDirection, PushDirection};
pub use context::ExecutionContext;
pub use locking::ExecutionLocks;
pub use shard::{DbShard, ShardCache};
