// src/config.rs

//! Manages server configuration: loading, resolving dynamic values, and validation.

use crate::core::acl::rules::AclRule;
use crate::core::acl::user::AclUser;
use crate::core::cluster::ClusterConfig;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::{Arc, Mutex};
use sysinfo::System;
use tracing::{info, warn};

/// A simple wrapper to allow cloning the config while it's behind a mutex for the `from_file` helper.
pub trait IntoMutex: Sized {
    fn into_mutex(self) -> Arc<Mutex<Self>>;
}

impl IntoMutex for Config {
    fn into_mutex(self) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(self))
    }
}

/// Represents the data structure of the separate ACL users file (e.g., users.json).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AclUsersFile {
    pub users: Vec<AclUser>,
}

/// Represents the different memory eviction strategies.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum EvictionPolicy {
    #[default]
    NoEviction,
    AllkeysLru,
    VolatileLru,
    AllkeysRandom,
    VolatileRandom,
    VolatileTtl,
    AllkeysLfu,
    VolatileLfu,
}

/// Holds safety-related configurations, like command circuit breakers.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SafetyConfig {
    /// Rejects commands that scan collections if the size exceeds this limit. `0` disables the check.
    #[serde(default = "default_max_collection_scan_keys")]
    pub max_collection_scan_keys: usize,
    /// Rejects set operations if the number of input keys exceeds this limit. `0` disables the check.
    #[serde(default = "default_max_set_operation_keys")]
    pub max_set_operation_keys: usize,
    /// The maximum execution time for a Lua script in milliseconds. `0` disables the timeout.
    #[serde(default = "default_script_timeout_ms")]
    pub script_timeout_ms: u64,
    /// The maximum memory a Lua script can allocate in megabytes. `0` disables the limit.
    #[serde(default = "default_script_memory_limit_mb")]
    pub script_memory_limit_mb: usize,
    /// If a key's size exceeds this value, `DEL` will behave like `UNLINK`. `0` disables this feature.
    #[serde(default = "default_auto_unlink_threshold")]
    pub auto_unlink_on_del_threshold: usize,
    /// Rejects BITOP if the largest source string exceeds this limit. `0` disables the check.
    #[serde(default = "default_max_bitop_alloc_size")]
    pub max_bitop_alloc_size: usize,
    /// Maximum size in bytes of a single bulk string in the RESP protocol.
    /// `0` falls back to the hard-coded default of 512 MB. Hard cap at 16 GB.
    #[serde(default)]
    pub max_bulk_string_size: usize,
    /// Number of Lua VMs in the pool. Each VM can execute a script independently
    /// so the engine does not become a serialized bottleneck. `0` falls back to
    /// the default of `max(1, num_cpus::get() / 2)`.
    #[serde(default)]
    pub lua_vm_pool_size: usize,
    /// If `true`, NaN scores sent to `ZADD` are rejected with an error rather than
    /// being stored as "equal to all other NaN scores".
    #[serde(default = "default_reject_nan_scores")]
    pub reject_nan_scores: bool,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            max_collection_scan_keys: default_max_collection_scan_keys(),
            max_set_operation_keys: default_max_set_operation_keys(),
            script_timeout_ms: default_script_timeout_ms(),
            script_memory_limit_mb: default_script_memory_limit_mb(),
            auto_unlink_on_del_threshold: default_auto_unlink_threshold(),
            max_bitop_alloc_size: default_max_bitop_alloc_size(),
            max_bulk_string_size: 0,
            lua_vm_pool_size: 0,
            reject_nan_scores: default_reject_nan_scores(),
        }
    }
}

/// Holds security-related configurations, such as network access controls.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SecurityConfig {
    pub password: Option<String>,
    /// A list of glob patterns for domains that `CACHE.FETCH` can connect to.
    /// If empty, all domains are allowed (default, for backward compatibility).
    #[serde(default = "default_allowed_fetch_domains")]
    pub allowed_fetch_domains: Vec<String>,
    /// If false, `CACHE.FETCH` will refuse to connect to private IP ranges (e.g., 127.0.0.1, 192.168.x.x)
    /// to prevent Server-Side Request Forgery (SSRF) attacks.
    #[serde(default)]
    pub allow_private_fetch_ips: bool,
}

fn default_allowed_fetch_domains() -> Vec<String> {
    vec![]
}
fn default_max_collection_scan_keys() -> usize {
    0
}
fn default_max_set_operation_keys() -> usize {
    0
}
fn default_script_timeout_ms() -> u64 {
    5000 // 5 seconds
}
fn default_script_memory_limit_mb() -> usize {
    32 // 32 MB
}
fn default_auto_unlink_threshold() -> usize {
    0
}
fn default_max_bitop_alloc_size() -> usize {
    128 * 1024 * 1024 // 128 MB
}
fn default_reject_nan_scores() -> bool {
    true
}

/// Configuration for Access Control List (ACL).
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AclConfig {
    #[serde(default)]
    pub enabled: bool,
    /// The user list is populated at startup from the `acl_file`.
    #[serde(skip, default = "default_acl_users")]
    pub users: Vec<AclUser>,
    #[serde(default = "default_acl_rules")]
    pub rules: Vec<AclRule>,
}

fn default_acl_users() -> Vec<AclUser> {
    vec![]
}
fn default_acl_rules() -> Vec<AclRule> {
    vec![]
}

/// Configuration for the Prometheus metrics exporter.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetricsConfig {
    /// If true, an HTTP server will be started to expose Prometheus metrics.
    #[serde(default)]
    pub enabled: bool,
    /// The port for the Prometheus metrics server.
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

fn default_metrics_port() -> u16 {
    8878
}

// Platform-specific module for detecting cgroup memory limits on Linux.
#[cfg(target_os = "linux")]
mod linux_memory {
    use super::*;

    pub fn get_cgroup_memory_limit() -> Option<u64> {
        let check = |path: &str, ver: &str| {
            fs::read_to_string(path).ok().and_then(|s| {
                s.trim().parse::<u64>().ok().and_then(|limit| {
                    if limit < u64::MAX / 2 {
                        info!("Detected cgroup {} memory limit: {} bytes", ver, limit);
                        Some(limit)
                    } else {
                        None
                    }
                })
            })
        };

        check("/sys/fs/cgroup/memory.max", "v2")
            .or_else(|| check("/sys/fs/cgroup/memory/memory.limit_in_bytes", "v1"))
    }
}

// Stub module for non-Linux operating systems.
#[cfg(not(target_os = "linux"))]
mod other_os_memory {
    pub fn get_cgroup_memory_limit() -> Option<u64> {
        None
    }
}

/// Gets the available memory, prioritizing cgroup limits on Linux over system memory.
fn get_available_memory() -> Result<u64> {
    #[cfg(target_os = "linux")]
    let cgroup_limit = linux_memory::get_cgroup_memory_limit();

    #[cfg(not(target_os = "linux"))]
    let cgroup_limit = other_os_memory::get_cgroup_memory_limit();

    if let Some(limit) = cgroup_limit {
        return Ok(limit);
    }

    let mut sys = System::new();
    sys.refresh_memory();
    let total_memory = sys.total_memory();
    warn!(
        "Could not detect cgroup memory limit. Using total system memory: {} bytes",
        total_memory
    );
    Ok(total_memory)
}

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
enum MaxMemoryConfig {
    Bytes(usize),
    String(String),
}

/// A raw representation of the config file before validation and resolution.
#[derive(Deserialize)]
struct RawConfig {
    #[serde(default = "default_host")]
    host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_log_level")]
    log_level: String,
    #[serde(default = "default_max_clients")]
    max_clients: usize,
    #[serde(default = "default_maxmemory_config")]
    maxmemory: MaxMemoryConfig,
    #[serde(default)]
    maxmemory_policy: EvictionPolicy,
    #[serde(default)]
    persistence: PersistenceConfig,
    #[serde(default)]
    replication: ReplicationConfig,
    #[serde(default = "default_databases")]
    databases: usize,
    #[serde(default)]
    cluster: ClusterConfig,
    #[serde(default)]
    tls: TlsConfig,
    #[serde(default)]
    safety: SafetyConfig,
    #[serde(default)]
    security: SecurityConfig,
    #[serde(default)]
    acl_file: Option<String>,
    #[serde(default)]
    acl: AclConfig,
    #[serde(default)]
    cache: CacheConfig,
    #[serde(default)]
    metrics: MetricsConfig,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    7878
}
fn default_databases() -> usize {
    16
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_max_clients() -> usize {
    10000
}
fn default_maxmemory_config() -> MaxMemoryConfig {
    MaxMemoryConfig::Bytes(512 * 1024 * 1024)
}

/// Configuration for TLS encryption.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_cert_path")]
    pub cert_path: String,
    #[serde(default = "default_key_path")]
    pub key_path: String,
}

fn default_cert_path() -> String {
    "spineldb.crt".to_string()
}
fn default_key_path() -> String {
    "spineldb.key".to_string()
}

/// Configuration for the Intelligent Cache feature.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CacheConfig {
    /// Items larger than this will be streamed to disk instead of being stored in memory.
    #[serde(default = "default_streaming_threshold")]
    pub streaming_threshold_bytes: usize,
    /// The directory path for storing on-disk cache files.
    #[serde(default = "default_disk_path")]
    pub on_disk_path: String,
    /// The maximum size of the on-disk cache in bytes. `0` means no limit.
    #[serde(default = "default_max_disk_size")]
    pub max_disk_size: u64,
    /// The maximum number of variants (from the Vary header) to store per cache key. `0` means no limit.
    #[serde(default = "default_max_variants_per_key")]
    pub max_variants_per_key: usize,
    /// The TTL in seconds for caching origin failures (negative caching). `0` disables it.
    #[serde(default = "default_negative_cache_ttl")]
    pub negative_cache_ttl_seconds: u64,
    /// The maximum number of concurrent file reads from the on-disk cache.
    #[serde(default = "default_on_disk_max_open_files")]
    pub on_disk_max_open_files: usize,
    /// How often (in seconds) the on-disk cache GC/compaction cycle runs.
    /// Lower values reduce the window in which orphan files can accumulate
    /// after a crash. Default: 600 (10 minutes).
    #[serde(default = "default_on_disk_gc_interval_secs")]
    pub on_disk_gc_interval_secs: u64,
}

fn default_streaming_threshold() -> usize {
    1024 * 1024 // 1 MB
}
fn default_disk_path() -> String {
    "spineldb_data/cache_files".to_string()
}
fn default_max_disk_size() -> u64 {
    0 // No limit
}
fn default_max_variants_per_key() -> usize {
    64
}
fn default_negative_cache_ttl() -> u64 {
    10 // 10 seconds
}
fn default_on_disk_max_open_files() -> usize {
    1024
}
fn default_on_disk_gc_interval_secs() -> u64 {
    600 // 10 minutes
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            streaming_threshold_bytes: default_streaming_threshold(),
            on_disk_path: default_disk_path(),
            max_disk_size: default_max_disk_size(),
            max_variants_per_key: default_max_variants_per_key(),
            negative_cache_ttl_seconds: default_negative_cache_ttl(),
            on_disk_max_open_files: default_on_disk_max_open_files(),
            on_disk_gc_interval_secs: default_on_disk_gc_interval_secs(),
        }
    }
}

/// Represents the final, validated, and resolved server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub host: String,
    pub port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    pub log_level: String,
    pub max_clients: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxmemory: Option<usize>,
    pub maxmemory_policy: EvictionPolicy,
    pub persistence: PersistenceConfig,
    pub replication: ReplicationConfig,
    pub databases: usize,
    #[serde(default)]
    pub cluster: ClusterConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub safety: SafetyConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acl_file: Option<String>,
    #[serde(default)]
    pub acl: AclConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            password: None,
            log_level: default_log_level(),
            max_clients: default_max_clients(),
            maxmemory: Some(512 * 1024 * 1024),
            maxmemory_policy: EvictionPolicy::default(),
            persistence: PersistenceConfig::default(),
            replication: ReplicationConfig::default(),
            databases: default_databases(),
            cluster: ClusterConfig::default(),
            tls: TlsConfig::default(),
            safety: SafetyConfig::default(),
            security: SecurityConfig::default(),
            acl_file: None,
            acl: AclConfig::default(),
            cache: CacheConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

/// Persistence-related settings for Append-Only File (AOF) and SpinelDB Database (SPLDB).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistenceConfig {
    pub aof_enabled: bool,
    pub aof_path: String,
    pub appendfsync: AppendFsync,
    #[serde(default = "default_auto_aof_rewrite_percentage")]
    pub auto_aof_rewrite_percentage: u64,
    #[serde(default = "default_auto_aof_rewrite_min_size")]
    pub auto_aof_rewrite_min_size: u64,
    #[serde(default = "default_aof_rewrite_buffer_limit")]
    pub aof_rewrite_buffer_limit: usize,
    /// The size of the AOF event channel. When the channel fills up, the
    /// command handler waits up to `aof_enqueue_timeout_ms` before either
    /// falling back to direct write or surfacing a backpressure error.
    /// `0` uses the default of 65 536.
    #[serde(default)]
    pub aof_channel_capacity: usize,
    /// Maximum time, in milliseconds, the command handler is willing to
    /// block while enqueuing an AOF event. `0` disables the timeout.
    #[serde(default = "default_aof_enqueue_timeout_ms")]
    pub aof_enqueue_timeout_ms: u64,
    pub spldb_enabled: bool,
    pub spldb_path: String,
    pub save_rules: Vec<SaveRule>,
}

fn default_auto_aof_rewrite_percentage() -> u64 {
    100
}
fn default_auto_aof_rewrite_min_size() -> u64 {
    64 * 1024 * 1024 // 64MB
}
fn default_aof_rewrite_buffer_limit() -> usize {
    256 * 1024 * 1024 // 256MB
}
fn default_aof_enqueue_timeout_ms() -> u64 {
    100
}

/// A rule defining when to automatically save the SPLDB file.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SaveRule {
    pub seconds: u64,
    pub changes: u64,
}

/// Defines the frequency of the `fsync` system call for AOF persistence.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AppendFsync {
    Always,
    EverySec,
    No,
}

/// Configuration specific to a Primary instance, for data safety policies.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ReplicationPrimaryConfig {
    #[serde(default = "default_min_replicas_to_write")]
    pub min_replicas_to_write: usize,
    #[serde(default = "default_min_replicas_max_lag")]
    pub min_replicas_max_lag: u64,
    /// If true, the primary will self-fence (enter read-only mode) if it loses contact
    /// with a quorum of its replicas. A strong defense against split-brain.
    #[serde(default)]
    pub fencing_on_replica_disconnect: bool,
    /// The timeout in seconds for the replica quorum fencing mechanism.
    #[serde(default = "default_replica_quorum_timeout")]
    pub replica_quorum_timeout_secs: u64,
    /// Capacity of the replication backlog in bytes. Larger values allow
    /// replicas to be disconnected longer before requiring a full resync.
    /// `0` uses the default of 2 MiB.
    #[serde(default)]
    pub backlog_capacity: usize,
}

fn default_min_replicas_to_write() -> usize {
    0
}
fn default_min_replicas_max_lag() -> u64 {
    10
}
fn default_replica_quorum_timeout() -> u64 {
    10
}

/// Defines the server's role in replication.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum ReplicationConfig {
    Primary(ReplicationPrimaryConfig),
    Replica {
        primary_host: String,
        primary_port: u16,
        #[serde(default)]
        tls_enabled: bool,
    },
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self::Primary(ReplicationPrimaryConfig::default())
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            aof_enabled: false,
            aof_path: default_aof_path(),
            appendfsync: default_appendfsync(),
            auto_aof_rewrite_percentage: default_auto_aof_rewrite_percentage(),
            auto_aof_rewrite_min_size: default_auto_aof_rewrite_min_size(),
            aof_rewrite_buffer_limit: default_aof_rewrite_buffer_limit(),
            aof_channel_capacity: 0,
            aof_enqueue_timeout_ms: default_aof_enqueue_timeout_ms(),
            spldb_enabled: true,
            spldb_path: default_spldb_path(),
            save_rules: default_save_rules(),
        }
    }
}

fn default_aof_path() -> String {
    "spineldb_data/spineldb.aof".to_string()
}
fn default_appendfsync() -> AppendFsync {
    AppendFsync::EverySec
}
fn default_spldb_path() -> String {
    "spineldb_data/dump.spldb".to_string()
}
fn default_save_rules() -> Vec<SaveRule> {
    vec![
        SaveRule {
            seconds: 900,
            changes: 1,
        },
        SaveRule {
            seconds: 300,
            changes: 10,
        },
        SaveRule {
            seconds: 60,
            changes: 10000,
        },
    ]
}

impl Config {
    /// Creates a new `Config` instance by reading and parsing a TOML file.
    pub fn from_file(path: &str) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file at '{path}'"))?;
        let raw_config: RawConfig = toml::from_str(&contents)
            .with_context(|| format!("Failed to parse TOML from '{path}'"))?;

        let available_memory = get_available_memory()?;
        let resolved_maxmemory = resolve_maxmemory(raw_config.maxmemory, available_memory)?;

        let security_config_clone = raw_config.security.clone();
        let password_option = raw_config.security.password;

        let mut config = Config {
            host: raw_config.host,
            port: raw_config.port,
            password: password_option,
            log_level: raw_config.log_level,
            max_clients: raw_config.max_clients,
            maxmemory: resolved_maxmemory,
            maxmemory_policy: raw_config.maxmemory_policy,
            persistence: raw_config.persistence,
            replication: raw_config.replication,
            databases: raw_config.databases,
            cluster: raw_config.cluster,
            tls: raw_config.tls,
            safety: raw_config.safety,
            security: security_config_clone,
            acl_file: raw_config.acl_file,
            acl: raw_config.acl,
            cache: raw_config.cache,
            metrics: raw_config.metrics,
        };

        config.validate(available_memory)?;
        Ok(config)
    }

    /// Validates the resolved configuration to ensure logical consistency.
    fn validate(&mut self, available_memory: u64) -> Result<()> {
        if self.port == 0 {
            return Err(anyhow!("port cannot be 0"));
        }
        if self.host.trim().is_empty() {
            return Err(anyhow!("host cannot be empty"));
        }
        if self.databases == 0 {
            return Err(anyhow!("databases cannot be 0"));
        }
        if self.max_clients == 0 {
            return Err(anyhow!("max_clients cannot be 0"));
        }

        if let Some(mem) = self.maxmemory {
            if mem > 0 && mem < 1_000_000 {
                warn!(
                    "low maxmemory setting: {} bytes. This may cause performance issues.",
                    mem
                );
            }
            if mem as u64 > available_memory {
                warn!(
                    "WARNING: maxmemory is set to {} bytes, which is greater than the total available memory ({} bytes).",
                    mem, available_memory
                );
            }
        }

        if self.persistence.aof_enabled {
            self.validate_persistence_path(&self.persistence.aof_path, "aof_path")?;
        }
        if self.persistence.spldb_enabled {
            self.validate_persistence_path(&self.persistence.spldb_path, "spldb_path")?;
        }

        if self.persistence.spldb_enabled {
            for (i, rule) in self.persistence.save_rules.iter().enumerate() {
                if rule.seconds == 0 {
                    return Err(anyhow!("invalid save rule #{}: seconds cannot be 0", i + 1));
                }
                if rule.changes == 0 {
                    return Err(anyhow!("invalid save rule #{}: changes cannot be 0", i + 1));
                }
            }
        }

        match &self.replication {
            ReplicationConfig::Replica { primary_port, .. } => {
                if *primary_port == 0 {
                    return Err(anyhow!("primary_port cannot be 0"));
                }
            }
            ReplicationConfig::Primary(primary_config) => {
                if primary_config.min_replicas_to_write > 0
                    && primary_config.min_replicas_max_lag == 0
                {
                    return Err(anyhow!(
                        "min_replicas_max_lag must be greater than 0 when min_replicas_to_write is set"
                    ));
                }
            }
        }

        if self.cluster.enabled {
            if self.cluster.failover_quorum == 0 {
                self.cluster.failover_quorum = 2;
                warn!(
                    "WARNING: cluster.failover_quorum is not set. Defaulting to 2. This is safe for a 3-master cluster. For other setups, please set it to (N/2 + 1) where N is the total number of masters."
                );
            }
            if self.cluster.failover_quorum == 1 {
                warn!(
                    "WARNING: cluster.failover_quorum is set to 1. This configuration is not fault-tolerant and cannot prevent split-brain."
                );
            }
        }

        if self.tls.enabled {
            if self.tls.cert_path.trim().is_empty() {
                return Err(anyhow!("tls.cert_path cannot be empty when TLS is enabled"));
            }
            if self.tls.key_path.trim().is_empty() {
                return Err(anyhow!("tls.key_path cannot be empty when TLS is enabled"));
            }
        }

        if self.metrics.enabled {
            if self.metrics.port == 0 {
                return Err(anyhow!("metrics.port cannot be 0"));
            }
            if self.metrics.port == self.port {
                return Err(anyhow!(
                    "metrics.port cannot be the same as the main server port"
                ));
            }
        }

        if self.safety.max_bulk_string_size > 16 * 1024 * 1024 * 1024 {
            return Err(anyhow!(
                "safety.max_bulk_string_size cannot exceed 16 GiB (got {} bytes)",
                self.safety.max_bulk_string_size
            ));
        }

        Ok(())
    }

    /// Helper to validate persistence paths.
    fn validate_persistence_path(&self, path_str: &str, name: &str) -> Result<()> {
        let path = std::path::Path::new(path_str);
        if path.is_dir() {
            return Err(anyhow!("{name} path '{path_str}' cannot be a directory."));
        }
        if let Some(parent) = path.parent()
            && parent.exists()
            && !parent.is_dir()
        {
            return Err(anyhow!(
                "Parent path for {} ('{}') exists but is not a directory.",
                name,
                parent.display()
            ));
        }
        Ok(())
    }
}

/// Resolves the `MaxMemoryConfig` into an `Option<usize>` representing bytes.
fn resolve_maxmemory(cfg: MaxMemoryConfig, available_memory: u64) -> Result<Option<usize>> {
    match cfg {
        MaxMemoryConfig::Bytes(b) => Ok(Some(b)),
        MaxMemoryConfig::String(s) => {
            let s_lower = s.to_lowercase();
            if let Some(percentage_str) = s_lower.strip_suffix('%') {
                let percentage: f64 = percentage_str
                    .parse()
                    .context("Invalid maxmemory percentage value")?;
                if !(0.0..=100.0).contains(&percentage) {
                    return Err(anyhow!(
                        "Invalid maxmemory percentage, must be between 0 and 100"
                    ));
                }
                let resolved_bytes = (available_memory as f64 * (percentage / 100.0)) as usize;
                info!(
                    "Resolved maxmemory '{}' to {} bytes ({:.2}% of total available {} bytes).",
                    s, resolved_bytes, percentage, available_memory
                );
                Ok(Some(resolved_bytes))
            } else if let Some(val_str) = s_lower.strip_suffix("gb") {
                parse_memory_string(&s, val_str, 1024 * 1024 * 1024)
            } else if let Some(val_str) = s_lower.strip_suffix('g') {
                parse_memory_string(&s, val_str, 1024 * 1024 * 1024)
            } else if let Some(val_str) = s_lower.strip_suffix("mb") {
                parse_memory_string(&s, val_str, 1024 * 1024)
            } else if let Some(val_str) = s_lower.strip_suffix('m') {
                parse_memory_string(&s, val_str, 1024 * 1024)
            } else if let Some(val_str) = s_lower.strip_suffix("kb") {
                parse_memory_string(&s, val_str, 1024)
            } else if let Some(val_str) = s_lower.strip_suffix('k') {
                parse_memory_string(&s, val_str, 1024)
            } else {
                let bytes: usize = s.parse().with_context(|| format!("Invalid maxmemory value '{s}'. Must be a number (bytes), a percentage (e.g., '50%'), or have a unit (e.g., '512mb')."))?;
                Ok(Some(bytes))
            }
        }
    }
}

/// Parses a string number with a unit (kb, mb, gb) and applies a multiplier.
fn parse_memory_string(
    original_str: &str,
    value_str: &str,
    multiplier: u64,
) -> Result<Option<usize>> {
    let value: u64 = value_str
        .trim()
        .parse()
        .with_context(|| format!("Invalid number in maxmemory config: '{original_str}'"))?;
    let result_u64 = value.saturating_mul(multiplier);
    if result_u64 > (usize::MAX as u64) {
        return Err(anyhow!(
            "maxmemory value '{}' is too large for this system's architecture (max is {} bytes)",
            original_str,
            usize::MAX
        ));
    }
    Ok(Some(result_u64 as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_policy_default_is_noeviction() {
        assert_eq!(EvictionPolicy::default(), EvictionPolicy::NoEviction);
    }

    #[test]
    fn test_safety_config_default_has_known_values() {
        let s = SafetyConfig::default();
        assert_eq!(s.max_collection_scan_keys, 0);
        assert_eq!(s.max_set_operation_keys, 0);
        assert_eq!(s.max_bulk_string_size, 0);
        assert!(s.reject_nan_scores);
    }

    #[test]
    fn test_security_config_default_has_no_password() {
        let s = SecurityConfig::default();
        assert!(s.password.is_none());
        assert!(s.allowed_fetch_domains.is_empty());
        assert!(!s.allow_private_fetch_ips);
    }

    #[test]
    fn test_acl_users_file_default() {
        let f = AclUsersFile::default();
        assert!(f.users.is_empty());
    }

    #[test]
    fn test_resolve_maxmemory_bytes_passthrough() {
        let cfg = MaxMemoryConfig::Bytes(1024);
        let r = resolve_maxmemory(cfg, 1_000_000).unwrap();
        assert_eq!(r, Some(1024));
    }

    #[test]
    fn test_resolve_maxmemory_plain_number() {
        let cfg = MaxMemoryConfig::String("2048".to_string());
        let r = resolve_maxmemory(cfg, 1_000_000).unwrap();
        assert_eq!(r, Some(2048));
    }

    #[test]
    fn test_resolve_maxmemory_kb_suffix() {
        let cfg = MaxMemoryConfig::String("512kb".to_string());
        let r = resolve_maxmemory(cfg, 0).unwrap();
        assert_eq!(r, Some(512 * 1024));
    }

    #[test]
    fn test_resolve_maxmemory_k_suffix() {
        let cfg = MaxMemoryConfig::String("8k".to_string());
        let r = resolve_maxmemory(cfg, 0).unwrap();
        assert_eq!(r, Some(8 * 1024));
    }

    #[test]
    fn test_resolve_maxmemory_mb_suffix() {
        let cfg = MaxMemoryConfig::String("128mb".to_string());
        let r = resolve_maxmemory(cfg, 0).unwrap();
        assert_eq!(r, Some(128 * 1024 * 1024));
    }

    #[test]
    fn test_resolve_maxmemory_m_suffix() {
        let cfg = MaxMemoryConfig::String("16m".to_string());
        let r = resolve_maxmemory(cfg, 0).unwrap();
        assert_eq!(r, Some(16 * 1024 * 1024));
    }

    #[test]
    fn test_resolve_maxmemory_gb_suffix() {
        let cfg = MaxMemoryConfig::String("2gb".to_string());
        let r = resolve_maxmemory(cfg, 0).unwrap();
        assert_eq!(r, Some(2 * 1024 * 1024 * 1024));
    }

    #[test]
    fn test_resolve_maxmemory_g_suffix() {
        let cfg = MaxMemoryConfig::String("1g".to_string());
        let r = resolve_maxmemory(cfg, 0).unwrap();
        assert_eq!(r, Some(1024 * 1024 * 1024));
    }

    #[test]
    fn test_resolve_maxmemory_uppercase_suffix() {
        let cfg = MaxMemoryConfig::String("256MB".to_string());
        let r = resolve_maxmemory(cfg, 0).unwrap();
        assert_eq!(r, Some(256 * 1024 * 1024));
    }

    #[test]
    fn test_resolve_maxmemory_percentage() {
        let cfg = MaxMemoryConfig::String("50%".to_string());
        let r = resolve_maxmemory(cfg, 1000).unwrap();
        assert_eq!(r, Some(500));
    }

    #[test]
    fn test_resolve_maxmemory_percentage_zero() {
        let cfg = MaxMemoryConfig::String("0%".to_string());
        let r = resolve_maxmemory(cfg, 1000).unwrap();
        assert_eq!(r, Some(0));
    }

    #[test]
    fn test_resolve_maxmemory_percentage_hundred() {
        let cfg = MaxMemoryConfig::String("100%".to_string());
        let r = resolve_maxmemory(cfg, 4096).unwrap();
        assert_eq!(r, Some(4096));
    }

    #[test]
    fn test_resolve_maxmemory_percentage_out_of_range_high() {
        let cfg = MaxMemoryConfig::String("150%".to_string());
        assert!(resolve_maxmemory(cfg, 1000).is_err());
    }

    #[test]
    fn test_resolve_maxmemory_percentage_out_of_range_negative() {
        let cfg = MaxMemoryConfig::String("-5%".to_string());
        assert!(resolve_maxmemory(cfg, 1000).is_err());
    }

    #[test]
    fn test_resolve_maxmemory_garbage_input_fails() {
        let cfg = MaxMemoryConfig::String("not-a-number".to_string());
        assert!(resolve_maxmemory(cfg, 0).is_err());
    }

    #[test]
    fn test_parse_memory_string_saturates_at_u64_max_on_overflow() {
        // The implementation uses saturating_mul then checks `> usize::MAX as u64`.
        // On 64-bit systems, usize::MAX == u64::MAX, so the saturated result is
        // accepted (but the returned value is the saturated maximum, not the
        // intended product). This test documents that behavior.
        let r = parse_memory_string("18000000000gb", "18000000000", 1024 * 1024 * 1024).unwrap();
        assert_eq!(r, Some(usize::MAX));
    }

    #[test]
    fn test_parse_memory_string_invalid_number() {
        let r = parse_memory_string("Xmb", "not-a-number", 1024 * 1024);
        assert!(r.is_err());
    }

    #[test]
    fn test_appendfsync_values_are_distinct() {
        assert_ne!(AppendFsync::Always, AppendFsync::EverySec);
        assert_ne!(AppendFsync::EverySec, AppendFsync::No);
        assert_ne!(AppendFsync::Always, AppendFsync::No);
    }

    #[test]
    fn test_save_rule_construction() {
        let r = SaveRule {
            seconds: 60,
            changes: 1000,
        };
        assert_eq!(r.seconds, 60);
        assert_eq!(r.changes, 1000);
    }

    #[test]
    fn test_eviction_policy_variants_all_distinct() {
        use EvictionPolicy::*;
        let all = [
            NoEviction,
            AllkeysLru,
            VolatileLru,
            AllkeysRandom,
            VolatileRandom,
            VolatileTtl,
            AllkeysLfu,
            VolatileLfu,
        ];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn test_eviction_policy_serde_kebab_case() {
        // The enum serializes as kebab-case strings.
        let s = serde_json::to_string(&EvictionPolicy::AllkeysLru).unwrap();
        assert_eq!(s, "\"allkeys-lru\"");

        let s = serde_json::to_string(&EvictionPolicy::VolatileTtl).unwrap();
        assert_eq!(s, "\"volatile-ttl\"");

        let s = serde_json::to_string(&EvictionPolicy::NoEviction).unwrap();
        assert_eq!(s, "\"no-eviction\"");

        // Roundtrip
        let d: EvictionPolicy = serde_json::from_str("\"allkeys-lfu\"").unwrap();
        assert_eq!(d, EvictionPolicy::AllkeysLfu);
    }

    #[test]
    fn test_appendfsync_variants_all_distinct() {
        use AppendFsync::*;
        assert_ne!(Always, EverySec);
        assert_ne!(EverySec, No);
        assert_ne!(Always, No);
    }

    #[test]
    fn test_into_mutex_creates_arc_mutex() {
        let cfg = Config::default();
        let m = cfg.into_mutex();
        // Mutex is reachable and Config is preserved.
        let guard = m.lock().unwrap();
        assert_eq!(guard.maxmemory_policy, EvictionPolicy::NoEviction);
    }

    #[test]
    fn test_resolve_maxmemory_zero_bytes_yields_zero() {
        let cfg = MaxMemoryConfig::Bytes(0);
        let r = resolve_maxmemory(cfg, 1_000_000).unwrap();
        assert_eq!(r, Some(0));
    }

    #[test]
    fn test_resolve_maxmemory_tb_suffix_unsupported() {
        // The current implementation only supports kb/k, mb/m, gb/g (not tb).
        let cfg = MaxMemoryConfig::String("1tb".to_string());
        assert!(resolve_maxmemory(cfg, 0).is_err());
    }
}
