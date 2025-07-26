// src/core/handler/mod.rs

// Declare the new actions submodule here, in the parent module file.
mod actions;
mod pipeline;

pub mod command_router;
pub mod safety_guard;
pub mod transaction_handler;
