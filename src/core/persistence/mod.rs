// src/core/persistence/mod.rs

//! This module contains all logic related to data persistence, including
//! Append-Only File (AOF) and SpinelDB Database (SPLDB) (SPLDB) mechanisms.
//!
//! It is responsible for loading data from disk on startup, saving data to disk
//! during runtime, and handling background processes like AOF rewriting.

// Declare the persistence sub-modules.
mod aof_loader;
mod aof_rewriter;
mod aof_writer;
pub mod spldb;
pub mod spldb_saver;

// Re-export the primary public types from the sub-modules.
// This creates a clean public facade for the persistence system.
pub use aof_loader::AofLoader;
pub use aof_rewriter::rewrite_aof;
pub use aof_writer::AofWriterTask;
