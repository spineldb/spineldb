// src/server/metrics_server.rs

use crate::core::metrics::gather_metrics;
use crate::core::state::ServerState;
use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::broadcast;
use tracing::{error, info};

/// Handles HTTP requests to the /metrics endpoint.
///
/// It updates dynamic gauges before gathering all registered metrics
/// and encoding them in the Prometheus text format.
async fn metrics_handler(state: Arc<ServerState>) -> impl IntoResponse {
    // Update gauges that change frequently before gathering.
    let total_memory: usize = state.dbs.iter().map(|db| db.get_current_memory()).sum();
    crate::core::metrics::MEMORY_USED_BYTES.set(total_memory as f64);

    let is_ro = state.is_read_only.load(Ordering::Relaxed);
    crate::core::metrics::IS_READ_ONLY.set(if is_ro { 1.0 } else { 0.0 });

    let aof_rewriting = state
        .persistence
        .aof_rewrite_state
        .lock()
        .await
        .is_in_progress;
    crate::core::metrics::AOF_REWRITE_IN_PROGRESS.set(if aof_rewriting { 1.0 } else { 0.0 });

    let spldb_saving = state.persistence.is_saving_spldb.load(Ordering::Relaxed);
    crate::core::metrics::SPLDB_SAVE_IN_PROGRESS.set(if spldb_saving { 1.0 } else { 0.0 });

    let body = gather_metrics();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

/// Runs a simple HTTP server to expose Prometheus metrics on /metrics.
pub async fn run_metrics_server(state: Arc<ServerState>, mut shutdown_rx: broadcast::Receiver<()>) {
    // Get the configured port from the shared state.
    let port = {
        let config = state.config.lock().await;
        config.metrics.port
    };

    let app = Router::new().route("/metrics", get(move || metrics_handler(state.clone())));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!(
        "Prometheus metrics server listening on http://{}/metrics",
        addr
    );

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind metrics server on port {}: {}", port, e);
            return;
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_rx.recv().await.ok();
            info!("Metrics server shutting down.");
        })
        .await
        .unwrap();
}
