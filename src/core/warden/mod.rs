// src/core/warden/mod.rs

//! The main module for SpinelDB's high-availability and monitoring system, "Warden".
//!
//! Warden runs as a separate process mode (`--warden`) and is responsible for:
//! - Monitoring the health of primary and replica SpinelDB instances.
//! - Detecting when a primary instance is down (Subjective Down and Objective Down).
//! - Coordinating with other Warden instances to reach a quorum.
//! - Triggering and managing an automated failover process to promote a replica to a new primary.
//! - Providing an API for clients to query the current address of a master.

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

// Declare all sub-modules that make up the Warden functionality.
pub mod client;
pub mod config;
pub mod failover;
pub mod listener;
pub mod state;
pub mod worker;

// Import the necessary structs and functions from our sub-modules.
use self::config::WardenConfig;
use self::listener::run_listener;
use self::state::{GlobalWardenState, MasterState};
use self::worker::MasterMonitor;

/// The main entry point for running SpinelDB in Warden mode.
pub async fn run(config_path: &str) -> Result<()> {
    // Load the configuration from the specified TOML file.
    let config = WardenConfig::from_file(config_path).await?;
    info!(
        "Ignis Warden starting on {}:{} with {} master(s) to monitor.",
        config.host,
        config.port,
        config.masters.len()
    );

    // A JoinSet to manage all spawned asynchronous tasks.
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    // Generate a unique 40-character hexadecimal run ID for this Warden instance.
    // This is used to identify this Warden when communicating with others.
    let mut runid_bytes = [0u8; 20];
    getrandom::fill(&mut runid_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to generate random run ID: {}", e))?;
    let my_run_id = hex::encode(runid_bytes);
    info!("Warden run ID: {}", my_run_id);

    // Determine the address this Warden should announce to its peers.
    let my_announce_addr: SocketAddr = format!(
        "{}:{}",
        config.announce_ip.as_ref().unwrap_or(&config.host),
        config.port
    )
    .parse()?;

    // Create the global, shared state for the Warden.
    // This state is wrapped in an Arc to be shared safely across all tasks.
    let global_state = Arc::new(GlobalWardenState {
        my_run_id,
        masters: DashMap::new(),
    });

    // Iterate through each master configuration and spawn a dedicated monitor task for it.
    for master_config in config.masters {
        let name = master_config.name.clone();
        info!(
            "Initializing monitor for master '{}' at {}:{}",
            &name, &master_config.ip, &master_config.port
        );

        // Create the specific state for this master, protected by a Mutex.
        let master_state = Arc::new(Mutex::new(MasterState::from(master_config)));

        // Insert this master's state into the global state map.
        global_state
            .masters
            .insert(name.clone(), master_state.clone());

        // Create and spawn the monitor task.
        let monitor =
            MasterMonitor::new(name, master_state, global_state.clone(), my_announce_addr);

        // Store master name before moving the monitor into the async block.
        let master_name_for_log = monitor.master_name().to_string();
        tasks.spawn(async move {
            monitor.run().await;
            // This task should run forever, if it exits it's a critical issue.
            // Return an error to signal this.
            Err(anyhow!(
                "MasterMonitor for {} exited unexpectedly.",
                master_name_for_log
            ))
        });
    }

    // Spawn the TCP listener task. This allows other clients or Wardens
    // to query this Warden for information (e.g., the current master address).
    tasks.spawn(run_listener(config.port, global_state.clone()));

    // Wait for any of the main tasks to complete. In normal operation, this loop
    // should not exit. If it does, it indicates a critical failure.
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {
                // This case is unlikely if tasks are designed to loop forever.
                warn!("A Warden task completed unexpectedly without an error.");
            }
            Ok(Err(e)) => {
                // A task returned a specific error.
                error!("A Warden task failed: {}", e);
            }
            Err(e) => {
                // A task panicked.
                error!("A Warden task panicked: {}", e);
            }
        }
    }

    // If the loop exits, it means all tasks have stopped, which is a server-level error.
    Err(anyhow!("All Warden tasks have terminated. Shutting down."))
}
