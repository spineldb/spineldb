// src/core/state/mod.rs

//! Defines the central `ServerState` struct and all related state components.
//! This module is broken down into logical parts for better organization.

pub mod cache;
mod client;
mod core;
mod persistence;
mod replication;
mod stats;

pub use cache::CacheState;
pub use client::*;
pub use core::{ServerInit, ServerState};
pub use persistence::*;
pub use replication::*;
pub use stats::StatsState;
