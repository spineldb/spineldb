// src/core/commands/json/mod.rs
//! Implements the native JSON command family, dispatching subcommands like JSON.GET and JSON.SET.

// Internal helper functions for JSON path parsing and value manipulation.
mod helpers;

// Public modules for the main dispatcher and each subcommand implementation.
pub mod command;
pub mod json_arrappend;
pub mod json_arrindex;
pub mod json_arrinsert;
pub mod json_arrlen;
pub mod json_arrpop;
pub mod json_arrtrim;
pub mod json_clear;
pub mod json_del;
pub mod json_get;
pub mod json_merge;
pub mod json_mget;
pub mod json_numincrby;
pub mod json_nummultby;
pub mod json_objkeys;
pub mod json_objlen;
pub mod json_set;
pub mod json_strappend;
pub mod json_strlen;
pub mod json_toggle;
pub mod json_type;

// Publicly re-export the main `Json` dispatcher struct as the primary entry point for this module.
pub use self::command::Json;
