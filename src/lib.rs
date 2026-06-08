// src/lib.rs

pub mod config;
pub mod connection;
pub mod core;
pub mod server;

// Re-export
pub use crate::core::warden;

#[cfg(test)]
pub(crate) mod test_helpers {
    use crate::config::Config;
    use crate::core::state::ServerState;
    use std::sync::Arc;
    use tracing_subscriber::filter::EnvFilter;
    use tracing_subscriber::reload;

    pub(crate) fn init_server_state(config: Config) -> Arc<ServerState> {
        let (_filter, reload_handle) =
            reload::Layer::<EnvFilter, tracing_subscriber::Registry>::new(EnvFilter::default());
        let reload_handle = Arc::new(reload_handle);
        let state_init = ServerState::initialize(config, reload_handle).unwrap();
        state_init.state
    }
}
