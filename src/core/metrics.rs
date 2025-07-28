// src/core/metrics.rs

//! Defines and registers Prometheus metrics for server monitoring.
//!
//! This module uses `lazy_static` to ensure that metrics are registered only once
//! globally for the entire application lifecycle.

use lazy_static::lazy_static;
use prometheus::{
    Counter, CounterVec, Gauge, Histogram, TextEncoder, register_counter, register_counter_vec,
    register_gauge, register_histogram,
};

lazy_static! {
    // --- Server-wide Gauges ---
    /// The number of clients currently connected to the server.
    pub static ref CONNECTED_CLIENTS: Gauge =
        register_gauge!("spineldb_connected_clients", "Number of currently connected clients.").unwrap();
    /// The total amount of memory allocated by the database keyspace.
    pub static ref MEMORY_USED_BYTES: Gauge =
        register_gauge!("spineldb_memory_used_bytes", "Total memory used by all databases in bytes.").unwrap();
    /// A boolean gauge indicating if the server is in read-only mode.
    pub static ref IS_READ_ONLY: Gauge =
        register_gauge!("spineldb_read_only", "Indicates if the server is in read-only mode (1 for true, 0 for false).").unwrap();
    /// A boolean gauge indicating if an AOF rewrite is in progress.
    pub static ref AOF_REWRITE_IN_PROGRESS: Gauge =
        register_gauge!("spineldb_aof_rewrite_in_progress", "AOF rewrite in progress (1 for true, 0 for false).").unwrap();
    /// A boolean gauge indicating if a background SPLDB save is in progress.
    pub static ref SPLDB_SAVE_IN_PROGRESS: Gauge =
        register_gauge!("spineldb_spldb_save_in_progress", "SPLDB save in progress (1 for true, 0 for false).").unwrap();


    // --- Server-wide Counters ---
    /// The total number of commands processed by the server since startup.
    pub static ref COMMANDS_PROCESSED_TOTAL: Counter =
        register_counter!("spineldb_commands_processed_total", "Total number of commands processed.").unwrap();
    /// The total number of connections accepted by the server since startup.
    pub static ref CONNECTIONS_RECEIVED_TOTAL: Counter =
        register_counter!("spineldb_connections_received_total", "Total number of connections received.").unwrap();
    /// The total number of keys expired by the active TTL manager.
    pub static ref EXPIRED_KEYS_TOTAL: Counter =
        register_counter!("spineldb_expired_keys_total", "Total number of keys expired proactively by the TTL manager.").unwrap();
    /// The total number of keys evicted due to the maxmemory limit.
    pub static ref EVICTED_KEYS_TOTAL: Counter =
        register_counter!("spineldb_evicted_keys_total", "Total number of keys evicted due to maxmemory limit.").unwrap();


    // --- Cache Counters ---
    /// The total number of successful cache lookups, labeled by the cache policy used.
    pub static ref CACHE_HITS_TOTAL: CounterVec =
        register_counter_vec!("spineldb_cache_hits_total", "Total number of cache hits, labeled by policy.", &["policy"]).unwrap();
    /// The total number of failed cache lookups, labeled by the cache policy used.
    pub static ref CACHE_MISSES_TOTAL: CounterVec =
        register_counter_vec!("spineldb_cache_misses_total", "Total number of cache misses, labeled by policy.", &["policy"]).unwrap();
    /// The total number of cache items evicted due to memory pressure.
    pub static ref CACHE_EVICTIONS_TOTAL: Counter =
        register_counter!("spineldb_cache_evictions_total", "Total number of cache keys evicted.").unwrap();


    // --- Histograms ---
    /// A histogram of command execution latencies.
    pub static ref COMMAND_LATENCY_SECONDS: Histogram =
        register_histogram!("spineldb_command_latency_seconds", "Latency of command processing in seconds.").unwrap();
}

/// Gathers all registered metrics and encodes them in the Prometheus text format.
pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode_to_string(&metric_families).unwrap()
}
