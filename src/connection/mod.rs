// src/connection/mod.rs

//! Manages the lifecycle of a single client TCP connection, including command
//! parsing, execution routing, and session state management.

// Declare the private sub-modules of the `connection` module.
mod guard;
mod handler;
mod session;

// Publicly re-export the primary types from the sub-modules.
// This creates a clean public API for the `connection` module, hiding the
// internal file structure from the rest of the crate.
pub use guard::ConnectionGuard;
pub use handler::ConnectionHandler;
pub use session::{SessionState, SubscriptionReceiver};

// A module-level type alias for convenience when accessing the global client map.
use dashmap::mapref::one::RefMut;
pub type ClientMapEntry<'a> = RefMut<'a, u64, crate::core::state::ClientStateTuple>;
