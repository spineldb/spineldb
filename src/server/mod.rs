// src/server/mod.rs

use crate::config::Config;
use anyhow::Result;
use std::sync::Arc;
use tracing_subscriber::{filter::EnvFilter, reload};

// Deklarasikan sub-modul dengan nama baru
mod connection_loop;
mod context;
mod initialization;
mod metrics_server;
mod spawner;
mod stream;

// Re-ekspor AnyStream
pub use stream::AnyStream;

/// The main server startup function, orchestrating all setup phases.
pub async fn run(
    config: Config,
    log_reload_handle: Arc<reload::Handle<EnvFilter, tracing_subscriber::Registry>>,
) -> Result<()> {
    // 1. Initialize server state, listener, TLS, etc.
    let mut server_context = initialization::setup(config, log_reload_handle).await?;

    // 2. Spawn all background tasks.
    // Panggil fungsi dari modul yang namanya sudah diubah
    spawner::spawn_all(&mut server_context).await?;

    // 3. Start the main connection acceptance loop. This function will run until shutdown.
    connection_loop::run(server_context).await;

    Ok(())
}
