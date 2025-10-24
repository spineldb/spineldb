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
        }
    }
}

/// Holds security-related configurations, such as network access controls.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SecurityConfig {
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
        // Cgroup v2
        if let Ok(limit_str) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
            if let Ok(limit) = limit_str.trim().parse::<u64>() {
                if limit < u64::MAX / 2 {
                    info!("Detected cgroup v2 memory limit: {} bytes", limit);
                    return Some(limit);
                }
            }
        }

        // Cgroup v1
        if let Ok(limit_str) = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
            if let Ok(limit) = limit_str.trim().parse::<u64>() {
                if limit < u64::MAX / 2 {
                    info!("Detected cgroup v1 memory limit: {} bytes", limit);
                    return Some(limit);
                }
            }
        }

        None
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
    password: Option<String>,
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

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            streaming_threshold_bytes: default_streaming_threshold(),
            on_disk_path: default_disk_path(),
            max_disk_size: default_max_disk_size(),
            max_variants_per_key: default_max_variants_per_key(),
            negative_cache_ttl_seconds: default_negative_cache_ttl(),
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

        let config = Config {
            host: raw_config.host,
            port: raw_config.port,
            password: raw_config.password,
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
            security: raw_config.security,
            acl_file: raw_config.acl_file,
            acl: raw_config.acl,
            cache: raw_config.cache,
            metrics: raw_config.metrics,
        };

        config.validate()?;
        Ok(config)
    }

    /// Validates the resolved configuration to ensure logical consistency.
    fn validate(&self) -> Result<()> {
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

        if let Some(mem) = self.maxmemory
            && mem > 0
            && mem < 1_000_000
        {
            warn!(
                "low maxmemory setting: {} bytes. This may cause performance issues.",
                mem
            );
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
                return Err(anyhow!("cluster.failover_quorum cannot be 0"));
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
