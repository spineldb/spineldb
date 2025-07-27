// src/server/connection_loop.rs

//! Contains the main server loop for accepting connections and handling graceful shutdown.

use super::context::ServerContext;
use super::stream::AnyStream;
use crate::connection::ConnectionHandler;
use crate::core::metrics;
use crate::core::persistence::spldb_saver::SpldbSaverTask;
use crate::core::state::{ClientInfo, ClientRole}; // Import ClientRole
use anyhow::anyhow;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

/// The main server loop that accepts connections and handles graceful shutdown.
pub async fn run(mut ctx: ServerContext) {
    let mut session_id_counter: u64 = 0;
    let mut client_tasks = JoinSet::new();

    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| anyhow!("Failed to register SIGINT handler: {}", e))
        .expect("Failed to create SIGINT stream");
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| anyhow!("Failed to register SIGTERM handler: {}", e))
        .expect("Failed to create SIGTERM stream");

    loop {
        tokio::select! {
            biased;

            _ = sigint.recv() => {
                info!("SIGINT received, initiating graceful shutdown.");
                break;
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received, initiating graceful shutdown.");
                break;
            }

            Some(res) = ctx.background_tasks.join_next() => {
                match res {
                    Ok(Ok(())) => warn!("A background task finished unexpectedly without an error."),
                    Ok(Err(e)) => { error!("CRITICAL: Background task failed: {}. Shutting down.", e); break; }
                    Err(e) => { error!("CRITICAL: Background task panicked: {e:?}. Shutting down."); break; }
                }
            },

            res = ctx.listener.accept() => {
                if let Ok((socket, addr)) = res {
                    info!("Accepted new connection from: {}", addr);
                    ctx.state.stats.increment_total_connections();
                    metrics::CONNECTIONS_RECEIVED_TOTAL.inc();
                    metrics::CONNECTED_CLIENTS.inc();

                    session_id_counter = session_id_counter.wrapping_add(1);
                    let session_id = session_id_counter;
                    let state_clone = ctx.state.clone();

                    let (conn_shutdown_tx, conn_shutdown_rx) = broadcast::channel(1);
                    let global_shutdown_rx = ctx.shutdown_tx.subscribe();

                    let client_info = Arc::new(Mutex::new(ClientInfo {
                        addr,
                        session_id,
                        name: None,
                        db_index: 0,
                        role: ClientRole::Normal, // Initialize new connections as 'Normal'.
                        created: Instant::now(),
                        last_command_time: Instant::now(),
                    }));
                    state_clone.clients.insert(session_id, (client_info, conn_shutdown_tx));

                    if let Some(acceptor) = ctx.acceptor.clone() {
                        client_tasks.spawn(async move {
                            match acceptor.accept(socket).await {
                                Ok(tls_stream) => {
                                    info!("TLS handshake successful for {addr}");
                                    let any_stream = AnyStream::Tls(Box::new(tls_stream));
                                    let mut handler = ConnectionHandler::new(any_stream, addr, state_clone, session_id, conn_shutdown_rx, global_shutdown_rx).await;
                                    if let Err(e) = handler.run().await { warn!("Connection from {} terminated unexpectedly: {}", addr, e); }
                                },
                                Err(e) => {
                                    warn!("TLS handshake error for {addr}: {e}");
                                }
                            }
                        });
                    } else {
                         client_tasks.spawn(async move {
                            let any_stream = AnyStream::Tcp(socket);
                            let mut handler = ConnectionHandler::new(any_stream, addr, state_clone, session_id, conn_shutdown_rx, global_shutdown_rx).await;
                            if let Err(e) = handler.run().await { warn!("Connection from {} terminated unexpectedly: {}", addr, e); }
                        });
                    }
                } else if let Err(e) = res {
                    error!("Failed to accept connection: {}", e);
                }
            },

            Some(res) = client_tasks.join_next() => {
                if let Err(e) = res {
                    if e.is_panic() {
                        error!("A client handler panicked: {e:?}");
                    }
                }
            },
        }
    }

    info!("Shutting down. Sending signal to all tasks.");
    if ctx.shutdown_tx.send(()).is_err() {
        error!("Failed to send shutdown signal. Some tasks may not terminate gracefully.");
    }

    client_tasks.shutdown().await;
    info!("All client connections closed.");

    let (spldb_enabled, aof_enabled, dirty_keys) = {
        let config = ctx.state.config.lock().await;
        (
            config.persistence.spldb_enabled,
            config.persistence.aof_enabled,
            ctx.state
                .persistence
                .dirty_keys_counter
                .load(Ordering::Relaxed),
        )
    };

    if spldb_enabled && !aof_enabled && dirty_keys > 0 {
        if let Some(handle) = ctx.state.persistence.bgsave_handle.lock().await.take() {
            info!("Waiting for in-progress BGSAVE to finish before final save...");
            let _ = handle.await;
        }

        info!(
            "Performing final SPLDB save on shutdown ({} dirty keys)...",
            dirty_keys
        );
        if let Err(e) = SpldbSaverTask::perform_save_logic(&ctx.state).await {
            error!("CRITICAL: Final SPLDB save on shutdown failed: {}", e);
        } else {
            info!("Final SPLDB save completed successfully.");
        }
    }

    if let Some(handle) = ctx.state.persistence.aof_rewrite_handle.lock().await.take() {
        info!("Waiting for AOF rewrite to complete...");
        if let Err(e) = handle.await {
            error!("AOF rewrite task finished with error: {e:?}");
        } else {
            info!("AOF rewrite task finished.");
        }
    }

    info!("Waiting for critical background operations (e.g., resharding) to finish...");
    // Wait indefinitely for critical tasks to avoid data corruption on shutdown.
    ctx.state.critical_tasks.lock().await.shutdown().await;
    info!("Critical operations finished.");

    info!("Waiting for background tasks to finish...");
    if tokio::time::timeout(Duration::from_secs(10), async {
        while ctx.background_tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        warn!("Timed out waiting for background tasks to finish cleanly.");
    };
    info!("Server shutdown complete.");
}
