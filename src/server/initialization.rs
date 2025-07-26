// src/server/initialization.rs

//! Handles the complete server initialization process, from configuration loading
//! to state setup and persistence loading.

use super::context::ServerContext;
use crate::config::Config;
use crate::core::persistence::{AofLoader, spldb::SpldbLoader};
use crate::core::state::ServerState;
use crate::core::tasks::cache_gc::garbage_collect_on_disk_cache;
use anyhow::{Result, anyhow};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_rustls::{TlsAcceptor, rustls};
use tracing::{error, info, warn};
use tracing_subscriber::{filter::EnvFilter, reload};

/// Initializes all server components before starting the main loop.
pub async fn setup(
    config: Config,
    log_reload_handle: Arc<reload::Handle<EnvFilter, tracing_subscriber::Registry>>,
) -> Result<ServerContext> {
    log_startup_info(&config);
    let (shutdown_tx, _) = broadcast::channel(1);

    let acceptor = setup_tls(&config).await?;

    let server_init = ServerState::initialize(config, log_reload_handle)?;
    let server_state = server_init.state.clone();
    info!("Server state initialized.");

    if server_state.config.lock().await.cluster.enabled {
        info!(
            "Server starting in CLUSTER mode. Node ID: {}",
            server_state
                .cluster
                .as_ref()
                .expect("Cluster state should be initialized")
                .my_id
        );
        warn!("--------------------------------------------------------------------------------");
        warn!("CLUSTER MODE WARNING:");
        warn!(" - The built-in, replica-initiated failover is susceptible to split-brain during");
        warn!("   network partitions. For production deployments, using the external Warden");
        warn!("   process (`--warden`) for failover management is STRONGLY RECOMMENDED.");
        warn!(" - Ensure all cluster nodes have their system clocks synchronized using NTP.");
        warn!("--------------------------------------------------------------------------------");
    } else {
        info!("Server starting in STANDALONE mode.");
    }

    load_persistence_data(&server_state).await?;

    // Garbage collect any orphaned on-disk cache files after loading state.
    if let Err(e) = garbage_collect_on_disk_cache(&server_state).await {
        warn!(
            "On-disk cache garbage collection failed: {}. This may lead to wasted disk space.",
            e
        );
    }

    let listener_config = server_state.config.lock().await;
    let listener = TcpListener::bind((listener_config.host.as_str(), listener_config.port)).await?;
    info!(
        "SpinelDB server listening on {}:{}",
        listener_config.host, listener_config.port
    );
    drop(listener_config);

    Ok(ServerContext {
        state: server_state,
        init_channels: server_init,
        listener,
        shutdown_tx,
        background_tasks: JoinSet::new(),
        acceptor,
    })
}

/// Sets up the TLS acceptor if TLS is enabled in the configuration.
async fn setup_tls(config: &Config) -> Result<Option<TlsAcceptor>> {
    if config.tls.enabled {
        info!("TLS is enabled. Loading certificate and key.");
        let certs = load_certs(&config.tls.cert_path)?;
        let key = load_key(&config.tls.key_path)?;
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        Ok(Some(TlsAcceptor::from(Arc::new(server_config))))
    } else {
        Ok(None)
    }
}

/// Loads TLS certificates from a PEM file.
fn load_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let cert_file = File::open(path)
        .map_err(|e| anyhow!("Failed to open certificate file '{}': {}", path, e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(anyhow!("No certificates found in '{}'", path));
    }
    Ok(certs)
}

/// Loads a private key from a PEM file.
fn load_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let key_file = File::open(path)
        .map_err(|e| anyhow!("Failed to open private key file '{}': {}", path, e))?;
    let mut key_reader = BufReader::new(key_file);
    rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow!("No private key found in key file '{}'", path))
}

/// Logs key configuration parameters at startup.
fn log_startup_info(config: &Config) {
    match config.maxmemory {
        Some(limit) => info!(
            "Memory limit set to {} bytes ({:.2} MB).",
            limit,
            limit as f64 / 1024.0 / 1024.0
        ),
        None => warn!("WARNING: No 'maxmemory' limit is active. Server memory is unbounded."),
    }
    info!("Server configured with {} databases.", config.databases);
}

/// Loads data from AOF or SPLDB based on the configuration.
async fn load_persistence_data(server_state: &Arc<ServerState>) -> Result<()> {
    let config = server_state.config.lock().await;

    // Sanity check for leftover temporary AOF files from a previous crashed rewrite.
    let aof_path_str = &config.persistence.aof_path;
    let aof_path = std::path::Path::new(aof_path_str);
    if let Some(parent_dir) = aof_path.parent() {
        if parent_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(parent_dir) {
                for entry in entries.flatten() {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if file_name.starts_with("temp-rewrite-")
                            && file_name.ends_with(
                                aof_path
                                    .file_name()
                                    .unwrap_or_default()
                                    .to_str()
                                    .unwrap_or_default(),
                            )
                        {
                            let msg = format!(
                                "FATAL: Found leftover AOF temp file '{}' from a previous crashed rewrite. Server is exiting to prevent data loss. Please manually inspect the files and restore the correct one by renaming it to '{}'.",
                                entry.path().display(),
                                aof_path.display()
                            );
                            error!("{}", msg);
                            return Err(anyhow!(msg));
                        }
                    }
                }
            }
        }
    }

    if config.persistence.aof_enabled {
        let aof_loader = AofLoader::new(config.persistence.clone());
        aof_loader.load_into(server_state).await?;
    } else if config.persistence.spldb_enabled {
        let spldb_loader = SpldbLoader::new(config.persistence.clone());
        spldb_loader.load_into(server_state).await?;
    } else {
        info!("No persistence method enabled. Starting with an empty state.");
    }
    info!("Persistence data loaded successfully.");
    Ok(())
}
