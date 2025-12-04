// src/main.rs

//! The main entry point for the SpinelDB server application.

use anyhow::Result;
use spineldb::config::Config;
use spineldb::server;
use std::env;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{filter::EnvFilter, prelude::*, reload};

#[tokio::main]
async fn main() -> Result<()> {
    // The `run_app` function is called directly, and the Tokio runtime is managed
    // by the `#[tokio::main]` macro. The previous implementation used a dedicated
    // thread with a large stack as a workaround for a recursive parsing vulnerability,
    // which has now been fixed.
    run_app().await
}

async fn run_app() -> Result<()> {
    // Define version information.
    const VERSION: &str = env!("CARGO_PKG_VERSION");

    // Collect command-line arguments to decide the execution mode.
    let args: Vec<String> = env::args().collect();

    // Handle the --version flag.
    if args.contains(&"--version".to_string()) {
        println!("SpinelDB version {VERSION}");
        return Ok(());
    }

    // Check if the --warden flag is present to start in Warden mode.
    if args.len() > 1 && args[1] == "--warden" {
        // --- Warden Mode ---

        // Validate that a configuration file path is provided.
        if args.len() != 3 {
            eprintln!("Usage: spineldb --warden /path/to/warden.toml");
            std::process::exit(1);
        }
        let config_path = &args[2];

        // Initialize logging for Warden mode.
        // It defaults to a more verbose level for sentinel-specific modules.
        let log_level = std::env::var("RUST_LOG")
            .unwrap_or_else(|_| "info,spineldb::core::warden=debug".to_string());

        // Setup logging with compact format and ANSI colors for Warden mode.
        tracing_subscriber::fmt()
            .with_env_filter(log_level)
            .compact()
            .with_ansi(true)
            .init();

        info!("Starting SpinelDB in Warden mode...");

        // Run the Warden main loop.
        if let Err(e) = spineldb::warden::run(config_path).await {
            error!("Warden runtime error: {}", e);
            return Err(e);
        }
    } else {
        // --- Normal Server Mode ---

        // Determine the configuration path.
        // It can be provided via a --config flag; otherwise, it defaults to "config.toml".
        let config_path = args
            .iter()
            .position(|arg| arg == "--config")
            .and_then(|i| args.get(i + 1))
            .map(|s| s.as_str())
            .unwrap_or("config.toml");

        // Load the server configuration from the determined path.
        // If loading fails, print the error and exit, as the server
        // cannot run without a valid configuration.
        let mut config = match Config::from_file(config_path) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("Failed to load configuration from \"{config_path}\": {e}");
                std::process::exit(1);
            }
        };

        // Override port if provided as a command-line argument
        if let Some(port_index) = args.iter().position(|arg| arg == "--port") {
            if let Some(port_str) = args.get(port_index + 1) {
                match port_str.parse::<u16>() {
                    Ok(port) => config.port = port,
                    Err(_) => {
                        eprintln!("Invalid port number: {port_str}");
                        std::process::exit(1);
                    }
                }
            } else {
                eprintln!("--port flag requires a value");
                std::process::exit(1);
            }
        }

        // Setup logging with reloading capabilities.
        // Get initial log level from env var or config.
        let initial_log_level =
            std::env::var("RUST_LOG").unwrap_or_else(|_| config.log_level.clone());

        // Create a reloadable filter layer.
        let (filter, reload_handle) = reload::Layer::new(EnvFilter::new(initial_log_level));

        // Initialize the global subscriber with the reload and formatting layers.
        tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .compact() // Use the compact, single-line format.
                    .with_ansi(true), // Enable ANSI color codes for log levels.
            )
            .init();

        // Store the handle in an Arc to be used for dynamic log level changes.
        let reload_handle = Arc::new(reload_handle);

        // Pass the handle to server::run
        if let Err(e) = server::run(config, reload_handle).await {
            error!("Server runtime error: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
