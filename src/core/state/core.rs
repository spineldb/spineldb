// src/core/state/core.rs

//! Defines the central `ServerState` struct, holding all shared server-wide state.

use super::cache::{CacheState, RevalidationJob};
use super::client::*;
use super::persistence::*;
use super::replication::*;
use super::stats::StatsState;
use crate::config::{AclConfig, AclUsersFile, Config};
use crate::core::SpinelDBError;
use crate::core::acl::enforcer::AclEnforcer;
use crate::core::blocking::BlockerManager;
use crate::core::cluster::gossip::GossipTaskMessage;
use crate::core::cluster::state::ClusterState;
use crate::core::database::Db;
use crate::core::events::{EventBus, PropagatedWork};
use crate::core::latency::LatencyMonitor;
use crate::core::pubsub::PubSubManager;
use crate::core::replication::backlog::ReplicationBacklog;
use crate::core::scripting::lua_manager::LuaManager;
use crate::core::stream_blocking::StreamBlockerManager;
use crate::core::tasks::lazy_free::LazyFreeItem;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, watch};
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::{filter::EnvFilter, reload};

/// Contains all initialized components required to spawn the server's background tasks.
/// This struct is created once during server initialization and then consumed by the spawner.
pub struct ServerInit {
    /// The fully initialized, shared server state.
    pub state: Arc<ServerState>,
    /// Receives events for the AOF writer task, if AOF is enabled.
    pub aof_event_rx: Option<mpsc::Receiver<PropagatedWork>>,
    /// A channel to signal the AOF writer to perform a periodic fsync.
    pub aof_fsync_request_rx: mpsc::Receiver<()>,
    /// A watch channel to notify the AOF writer that a rewrite has completed.
    pub aof_rewrite_complete_rx: watch::Receiver<()>,
    /// A channel to send items for asynchronous deallocation (e.g., from `UNLINK`).
    pub lazy_free_rx: mpsc::Receiver<Vec<LazyFreeItem>>,
    /// A channel for command handlers to send messages to the cluster gossip task.
    pub cluster_gossip_rx: mpsc::Receiver<GossipTaskMessage>,
    /// A broadcast channel to signal replication workers to reconfigure (e.g., after failover).
    pub replication_reconfigure_rx: broadcast::Receiver<()>,
    /// Receives jobs for the background cache revalidation worker.
    pub cache_revalidation_rx: mpsc::Receiver<RevalidationJob>,
}

/// The central struct holding all shared, server-wide state.
/// This struct is wrapped in an `Arc` and passed to nearly every task and
/// connection handler, providing a single source of truth for the server's configuration
/// and dynamic state.
#[derive(Debug)]
pub struct ServerState {
    // --- Core Components ---
    /// A vector of all databases, each sharded internally. Wrapped in `Arc` for shared access.
    pub dbs: Vec<Arc<Db>>,
    /// A map of all active client connections, keyed by a unique session ID.
    /// Stores client metadata and a shutdown sender for targeted connection termination.
    pub clients: ClientMap,
    /// The server's runtime configuration, wrapped in a Mutex to allow for dynamic changes
    /// via the `CONFIG SET` command.
    pub config: Arc<Mutex<Config>>,
    /// An atomic flag for administratively enabling read-only mode (e.g., during maintenance).
    pub is_read_only: Arc<AtomicBool>,
    /// An atomic flag to enable read-only mode in case of critical data consistency issues.
    pub is_emergency_read_only: AtomicBool,
    /// A flag set by a master when it loses contact with the cluster quorum.
    /// This is the primary self-fencing mechanism to prevent split-brain.
    pub is_read_only_due_to_quorum_loss: Arc<AtomicBool>,
    /// An atomic counter for tracking in-flight `EVALSHA` commands to prevent race conditions with `SCRIPT FLUSH`.
    pub evalsha_in_flight: Arc<AtomicUsize>,
    /// The manager for all publish-subscribe channels and patterns.
    pub pubsub: PubSubManager,
    /// Manages Lua scripts for `EVAL` and `EVALSHA`.
    pub scripting: Arc<LuaManager>,
    /// The central event bus that propagates write commands to the AOF and replication subsystems.
    pub event_bus: Arc<EventBus>,
    /// Manages clients blocked on list/zset commands (e.g., `BLPOP`).
    pub blocker_manager: Arc<BlockerManager>,
    /// Manages clients blocked on stream commands (e.g., `XREAD BLOCK`).
    pub stream_blocker_manager: Arc<StreamBlockerManager>,
    /// A circular buffer storing recent commands for partial replication sync.
    pub replication_backlog: ReplicationBacklog,
    /// A map storing the runtime state of all connected replicas.
    pub replica_states: Arc<DashMap<SocketAddr, ReplicaStateInfo>>,
    /// A map of locks to prevent concurrent full sync operations for the same replica.
    pub replica_sync_locks: Arc<DashMap<SocketAddr, Arc<Mutex<()>>>>,
    /// A receiver that gets notified whenever the primary's replication offset changes.
    pub replication_offset_receiver: watch::Receiver<u64>,
    /// A sender to signal replication workers to reconfigure (e.g., after failover).
    pub replication_reconfigure_tx: broadcast::Sender<()>,
    /// The state of the cluster, if enabled. `None` in standalone mode.
    pub cluster: Option<Arc<ClusterState>>,
    /// A sender for command handlers to send messages to the cluster gossip task.
    pub cluster_gossip_tx: mpsc::Sender<GossipTaskMessage>,
    /// The ACL configuration, wrapped for concurrent read/write access to allow `ACL` commands.
    pub acl_config: RwLock<Arc<AclConfig>>,
    /// The ACL enforcer, which holds the parsed, optimized ACL rules for efficient permission checks.
    pub acl_enforcer: RwLock<Arc<AclEnforcer>>,
    /// A handle to the logging filter, allowing for dynamic log level changes via `CONFIG SET`.
    pub log_reload_handle: Arc<reload::Handle<EnvFilter, tracing_subscriber::Registry>>,
    /// The latency monitoring system for `SLOWLOG` and `LATENCY` commands.
    pub latency_monitor: LatencyMonitor,
    /// A JoinSet to track critical, long-running tasks (e.g., CLUSTER RESHARD) for graceful shutdown.
    pub critical_tasks: Arc<Mutex<JoinSet<()>>>,

    // --- Sub-State Structs ---
    /// Holds all state related to persistence (AOF/SPLDB).
    pub persistence: PersistenceState,
    /// Holds all state related to replication (primary/replica roles, offsets).
    pub replication: ReplicationState,
    /// Holds all state related to the Intelligent Cache feature.
    pub cache: CacheState,
    /// Holds all server-wide statistics.
    pub stats: StatsState,
}

impl ServerState {
    /// Initializes the entire server state from the given configuration.
    /// This is the main factory function for creating the server's shared context.
    pub fn initialize(
        config: Config,
        log_reload_handle: Arc<reload::Handle<EnvFilter, tracing_subscriber::Registry>>,
    ) -> Result<ServerInit, SpinelDBError> {
        // Generate a unique run ID for this server instance, used for replication.
        let mut replid_bytes = [0u8; 20];
        getrandom::fill(&mut replid_bytes).map_err(|e| SpinelDBError::Internal(e.to_string()))?;
        let master_replid = hex::encode(replid_bytes);

        // Initialize channels for inter-task communication.
        let (event_bus, aof_event_rx) = EventBus::new(config.persistence.aof_enabled);
        let (fsync_tx, fsync_rx) = mpsc::channel(1);
        let (rewrite_complete_tx, rewrite_complete_rx) = watch::channel(());
        let (replication_backlog, replication_offset_receiver) = ReplicationBacklog::new();
        let (lazy_free_tx, lazy_free_rx) = mpsc::channel(128);
        let (cluster_gossip_tx, cluster_gossip_rx) = mpsc::channel(128);
        let (replication_reconfigure_tx, replication_reconfigure_rx) = broadcast::channel(1);

        const CACHE_REVALIDATION_CHANNEL_CAPACITY: usize = 128;
        let (reval_tx, reval_rx) = mpsc::channel(CACHE_REVALIDATION_CHANNEL_CAPACITY);

        // Initialize all databases.
        let dbs = (0..config.databases).map(|_| Arc::new(Db::new())).collect();

        // Initialize cluster state if enabled.
        let cluster = if config.cluster.enabled {
            let cluster_config_path = config.cluster.config_file.clone();
            let loaded_state_result = ClusterState::from_file(&cluster_config_path, &config);
            let final_cluster_state = match loaded_state_result {
                Ok(state) => Ok(state),
                Err(e) => {
                    warn!(
                        "Could not load cluster config file '{}': {}. Starting with a fresh state.",
                        cluster_config_path, e
                    );
                    ClusterState::new(&config)
                }
            }?;
            Some(Arc::new(final_cluster_state))
        } else {
            None
        };

        // Initialize ACL configuration, loading users from the specified file if enabled.
        let mut final_acl_config = config.acl.clone();
        if config.acl.enabled {
            if let Some(path) = &config.acl_file {
                info!("ACL is enabled. Attempting to load users from '{}'.", path);
                match std::fs::read_to_string(path) {
                    Ok(contents) => match serde_json::from_str::<AclUsersFile>(&contents) {
                        Ok(loaded_file) => {
                            final_acl_config.users = loaded_file.users;
                            info!(
                                "Successfully loaded {} ACL users.",
                                final_acl_config.users.len()
                            );
                        }
                        Err(e) => {
                            let err_msg = format!(
                                "Failed to parse ACL file '{path}': {e}. Server is starting with no users. Please fix the file."
                            );
                            error!("{err_msg}");
                            return Err(SpinelDBError::Internal(err_msg));
                        }
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        info!(
                            "ACL file '{}' not found. Starting with no users. It will be created on `ACL SAVE`.",
                            path
                        );
                    }
                    Err(e) => {
                        let err_msg = format!(
                            "Failed to read ACL file '{path}': {e}. Server is starting with no users."
                        );
                        error!("{err_msg}");
                        return Err(SpinelDBError::Internal(err_msg));
                    }
                }
            } else {
                warn!(
                    "ACL is enabled but 'acl_file' is not configured in config.toml. No users will be loaded and `ACL SAVE` will fail."
                );
            }
        }

        // Assemble the final ServerState struct.
        let state = Arc::new(Self {
            dbs,
            clients: Arc::new(DashMap::new()),
            config: Arc::new(Mutex::new(config)),
            is_read_only: Arc::new(AtomicBool::new(false)),
            is_emergency_read_only: AtomicBool::new(false),
            is_read_only_due_to_quorum_loss: Arc::new(AtomicBool::new(false)),
            pubsub: PubSubManager::new(),
            evalsha_in_flight: Arc::new(AtomicUsize::new(0)),
            scripting: Arc::new(LuaManager::new()),
            event_bus: Arc::new(event_bus),
            blocker_manager: Arc::new(BlockerManager::new()),
            stream_blocker_manager: Arc::new(StreamBlockerManager::new()),
            replication_backlog,
            replica_states: Arc::new(DashMap::new()),
            replica_sync_locks: Arc::new(DashMap::new()),
            replication_offset_receiver,
            replication_reconfigure_tx,
            cluster,
            cluster_gossip_tx,
            acl_config: RwLock::new(Arc::new(final_acl_config.clone())),
            acl_enforcer: RwLock::new(Arc::new(AclEnforcer::new(&final_acl_config))),
            log_reload_handle,
            latency_monitor: LatencyMonitor::new(),
            critical_tasks: Arc::new(Mutex::new(JoinSet::new())),
            persistence: PersistenceState::new(fsync_tx, rewrite_complete_tx, lazy_free_tx),
            replication: ReplicationState::new(master_replid),
            cache: CacheState::new(reval_tx),
            stats: StatsState::new(),
        });

        // Load persisted poisoned masters state from disk.
        state.replication.load_poisoned_masters_from_disk();

        // Return the initialized state and channels needed for spawning background tasks.
        Ok(ServerInit {
            state,
            aof_event_rx,
            aof_fsync_request_rx: fsync_rx,
            aof_rewrite_complete_rx: rewrite_complete_rx,
            lazy_free_rx,
            cluster_gossip_rx,
            replication_reconfigure_rx,
            cache_revalidation_rx: reval_rx,
        })
    }

    /// Retrieves a reference to a specific database by its index.
    pub fn get_db(&self, db_index: usize) -> Option<Arc<Db>> {
        self.dbs.get(db_index).cloned()
    }

    /// Sets the server's read-only mode for administrative reasons.
    pub fn set_read_only(&self, value: bool, reason: &str) {
        if value {
            tracing::warn!("Server entering read-only mode. Reason: {}", reason);
        } else {
            tracing::info!("Server exiting read-only mode.");
        }
        self.is_read_only.store(value, Ordering::SeqCst);
    }

    /// Sets the server's read-only mode due to quorum loss (self-fencing).
    /// This is a critical safety mechanism in cluster mode.
    pub fn set_quorum_loss_read_only(&self, value: bool, reason: &str) {
        if value {
            tracing::error!(
                "FENCING: Server entering read-only mode. Reason: {}",
                reason
            );
        } else {
            tracing::info!("FENCING: Server exiting read-only mode. Reason: {}", reason);
        }
        self.is_read_only_due_to_quorum_loss
            .store(value, Ordering::SeqCst);
    }
}
