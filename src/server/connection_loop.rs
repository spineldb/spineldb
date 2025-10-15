// src/server/connection_loop.rs

//! Contains the main server loop for accepting connections and handling graceful shutdown.

use super::context::ServerContext;
use super::stream::AnyStream;
use crate::connection::ConnectionHandler;
use crate::core::metrics;
use crate::core::persistence::spldb_saver::SpldbSaverTask;
use crate::core::state::{ClientInfo, ClientRole};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

// Platform-specific signal handling imports
#[cfg(windows)]
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

/// Waits for a shutdown signal based on the operating system.
/// On Unix, it listens for SIGINT and SIGTERM.
/// On Windows, it listens for Ctrl+C.
async fn await_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to create SIGINT stream");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to create SIGTERM stream");
        tokio::select! {
            _ = sigint.recv() => info!("SIGINT received, initiating graceful shutdown."),
            _ = sigterm.recv() => info!("SIGTERM received, initiating graceful shutdown."),
        }
    }

    #[cfg(windows)]
    {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Ctrl-C received, initiating graceful shutdown.");
    }
}

/// The main server loop that accepts connections and handles graceful shutdown.
pub async fn run(mut ctx: ServerContext) {
    let mut session_id_counter: u64 = 0;
    let mut client_tasks = JoinSet::new();

    loop {
        tokio::select! {
            biased; // Prioritize shutdown signals over other events.

            // Wait for a shutdown signal.
            _ = await_shutdown_signal() => {
                break;
            },

            // Monitor background tasks for unexpected termination.
            Some(res) = ctx.background_tasks.join_next() => {
                match res {
                    Ok(Ok(())) => info!("A background task has completed. This is expected for tasks whose features are disabled."),
                    Ok(Err(e)) => { error!("CRITICAL: Background task failed: {}. Shutting down.", e); break; }
                    Err(e) => { error!("CRITICAL: Background task panicked: {e:?}. Shutting down."); break; }
                }
            },

            // Wait for a connection permit to become available.
            permit = ctx.connection_permits.clone().acquire_owned() => {
                if permit.is_err() {
                    // The semaphore has been closed, which means we are shutting down.
                    break;
                }
                let permit = permit.unwrap(); // The permit is now owned by this scope.

                // A permit is available, now accept the new incoming TCP connection.
                match ctx.listener.accept().await {
                    Ok((socket, addr)) => {
                        info!("Accepted new connection from: {}", addr);
                        ctx.state.stats.increment_total_connections();
                        metrics::CONNECTIONS_RECEIVED_TOTAL.inc();
                        metrics::CONNECTED_CLIENTS.inc();

                        session_id_counter = session_id_counter.wrapping_add(1);
                        let session_id = session_id_counter;
                        let state_clone = ctx.state.clone();

                        // Create per-connection and global shutdown channels.
                        let (conn_shutdown_tx, conn_shutdown_rx) = broadcast::channel(1);
                        let global_shutdown_rx = ctx.shutdown_tx.subscribe();

                        // Register the new client in the global state.
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

                        // Spawn a task to handle the new connection, including TLS handshake if enabled.
                        // The connection permit is moved into the task and will be released when the task (and thus the connection) ends.
                        if let Some(acceptor) = ctx.acceptor.clone() {
                            client_tasks.spawn(async move {
                                let _permit = permit;
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
                                let _permit = permit;
                                let any_stream = AnyStream::Tcp(socket);
                                let mut handler = ConnectionHandler::new(any_stream, addr, state_clone, session_id, conn_shutdown_rx, global_shutdown_rx).await;
                                if let Err(e) = handler.run().await { warn!("Connection from {} terminated unexpectedly: {}", addr, e); }
                            });
                        }
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}. Retrying shortly...", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            },

            // Reap completed client tasks.
            Some(res) = client_tasks.join_next() => {
                if let Err(e) = res
                    && e.is_panic() {
                        error!("A client handler panicked: {e:?}");
                    }
            },
        }
    }

    // --- Graceful Shutdown Sequence ---
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

    // Perform a final SPLDB save if it's the only persistence method and there are unsaved changes.
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

    // Wait for a potentially ongoing AOF rewrite to complete.
    if let Some(handle) = ctx.state.persistence.aof_rewrite_handle.lock().await.take() {
        info!("Waiting for AOF rewrite to complete...");
        if let Err(e) = handle.await {
            error!("AOF rewrite task finished with error: {e:?}");
        } else {
            info!("AOF rewrite task finished.");
        }
    }

    // Wait for critical operations like cluster resharding to finish.
    info!("Waiting for critical background operations (e.g., resharding) to finish...");
    ctx.state.critical_tasks.lock().await.shutdown().await;
    info!("Critical operations finished.");

    // Wait for all other background tasks to terminate, with a timeout.
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
