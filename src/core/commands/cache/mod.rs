// src/core/commands/cache/mod.rs

mod helpers;

// Modules for each subcommand
pub mod cache_bypass;
pub mod cache_fetch;
pub mod cache_get;
pub mod cache_info;
pub mod cache_lock;
pub mod cache_policy;
pub mod cache_proxy;
pub mod cache_purge;
pub mod cache_purgetag;
pub mod cache_set;
pub mod cache_softpurge;
pub mod cache_softpurgetag;
pub mod cache_stats;

// Export the main dispatcher struct
pub mod command;
pub use self::command::Cache;
