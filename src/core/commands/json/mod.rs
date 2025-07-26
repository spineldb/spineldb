// src/core/commands/json/mod.rs

// This module contains the implementation for native JSON commands.

mod helpers;

// These modules now contain the implementation details for subcommands.
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

// Only export the main dispatcher struct.
pub use self::command::Json;
