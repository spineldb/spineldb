// src/core/cluster/state.rs

//! Manages the shared state of the cluster, including node information,
//! slot mappings, and failover status.

use super::slot::NUM_SLOTS;
use crate::config::{Config, IntoMutex, ReplicationConfig};
use crate::core::SpinelDBError;
use crate::core::state::ServerState;
use bitflags::bitflags;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// The role of a node in the cluster.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub enum NodeRole {
    Primary,
    Replica,
}

bitflags! {
    /// Flags representing the state and role of a cluster node, compatible with SpinelDB.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub struct NodeFlags: u16 {
        const MYSELF         = 1 << 0;  // The node is this server instance.
        const PRIMARY        = 1 << 1;  // The node is a primary (master).
        const REPLICA        = 1 << 2;  // The node is a replica (slave).
        const PFAIL          = 1 << 3;  // Possible failure (unconfirmed).
        const FAIL           = 1 << 4;  // Confirmed failure.
        const HANDSHAKE      = 1 << 5;  // Node is in handshake, not yet part of the cluster.
        const NOADDR         = 1 << 6;  // Node address is unknown.
        const MIGRATING      = 1 << 7;  // Node is migrating a slot to another node.
        const IMPORTING      = 1 << 8;  // Node is importing a slot from another node.
    }
}

/// Represents the configuration and static state of a node, gossiped and persisted.
#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct ClusterNode {
    pub id: String,
    pub addr: String,
    pub bus_addr: String,
    pub flags_raw: u16,
    pub replica_of: Option<String>,
    pub slots: BTreeSet<u16>,
    pub config_epoch: u64,
    #[serde(default)]
    pub replication_offset: u64,
    /// Stores slots this node is migrating to another. Key: slot, Value: destination node_id.
    #[serde(default)]
    pub migrating_slots: BTreeMap<u16, String>,
    /// Stores slots this node is importing from another. Key: slot, Value: source node_id.
    #[serde(default)]
    pub importing_slots: BTreeMap<u16, String>,
}

impl ClusterNode {
    /// Gets the state flags for this node.
    pub fn get_flags(&self) -> NodeFlags {
        NodeFlags::from_bits_truncate(self.flags_raw)
    }
    /// Sets the state flags for this node.
    pub fn set_flags(&mut self, flags: NodeFlags) {
        self.flags_raw = flags.bits();
    }
}

/// Represents the runtime state of a node, which is not persisted or gossiped.
#[derive(Debug, Clone)]
pub struct NodeRuntimeState {
    pub node_info: ClusterNode,
    pub ping_sent: Option<Instant>,
    pub pong_received: Option<Instant>,
    /// Tracks which nodes have reported this node as PFAIL. Key: reporter_id.
    pub pfail_reports: HashMap<String, Instant>,
}

/// A helper struct for serializing the essential cluster state to a file.
#[derive(Debug, Serialize, Deserialize)]
struct SerializableClusterState {
    my_id: String,
    current_epoch: u64,
    nodes: Vec<ClusterNode>,
}

/// `ClusterState` is the main container for all cluster-related information on this node.
#[derive(Debug)]
pub struct ClusterState {
    /// The unique 40-character hexadecimal run ID of this node.
    pub my_id: String,
    /// The current configuration epoch of the cluster, used for failover ordering.
    pub current_epoch: AtomicU64,
    /// The last used epoch for a `CACHE.PURGETAG` operation.
    pub last_purge_epoch: AtomicU64,
    /// A map of all known nodes in the cluster, keyed by their unique run ID.
    pub nodes: DashMap<String, NodeRuntimeState>,
    /// A mapping of each of the 16384 hash slots to the ID of the node that owns it.
    pub slots_map: [RwLock<Option<String>>; NUM_SLOTS],
    /// The file path for the persisted cluster configuration (`nodes.conf`).
    pub config_file_path: String,
    // --- Failover-related atomics for replica-initiated failovers ---
    pub last_vote_epoch: AtomicU64,
    pub failover_auth_time: AtomicU64,
    pub failover_auth_count: AtomicU64,
    pub failover_auth_rank: AtomicU64,
    pub failover_auth_epoch: AtomicU64,
}

impl ClusterState {
    /// Creates a new, fresh `ClusterState` for a node starting for the first time.
    pub fn new(config: &Config) -> Result<Self, SpinelDBError> {
        let my_id = hex::encode(rand::random::<[u8; 20]>());
        let slots_map = std::array::from_fn(|_| RwLock::new(None));
        let nodes = DashMap::new();

        let my_addr = config
            .cluster
            .announce_ip
            .clone()
            .unwrap_or_else(|| config.host.clone());
        let my_port = config.cluster.announce_port.unwrap_or(config.port);

        let my_bus_port = match u32::from(config.port)
            .checked_add(u32::from(config.cluster.bus_port_offset))
        {
            Some(port_u32) if port_u32 <= u32::from(u16::MAX) => {
                config.cluster.announce_bus_port.unwrap_or(port_u32 as u16)
            }
            _ => {
                let calculated_port =
                    u32::from(config.port) + u32::from(config.cluster.bus_port_offset);

                let err_msg = format!(
                    "Calculated cluster bus port ({calculated_port}) exceeds the valid range (max 65535). Please check 'port' and 'bus_port_offset' settings."
                );
                error!("FATAL: {}", err_msg);
                return Err(SpinelDBError::Internal(err_msg));
            }
        };

        let myself_info = ClusterNode {
            id: my_id.clone(),
            addr: format!("{my_addr}:{my_port}"),
            bus_addr: format!("{my_addr}:{my_bus_port}"),
            flags_raw: (NodeFlags::MYSELF | NodeFlags::PRIMARY).bits(),
            replica_of: None,
            slots: BTreeSet::new(),
            config_epoch: 0,
            replication_offset: 0,
            migrating_slots: BTreeMap::new(),
            importing_slots: BTreeMap::new(),
        };
        let myself_runtime = NodeRuntimeState {
            node_info: myself_info,
            ping_sent: None,
            pong_received: Some(Instant::now()),
            pfail_reports: HashMap::new(),
        };
        nodes.insert(my_id.clone(), myself_runtime);

        Ok(Self {
            my_id,
            current_epoch: AtomicU64::new(0),
            last_purge_epoch: AtomicU64::new(0),
            nodes,
            slots_map,
            config_file_path: config.cluster.config_file.clone(),
            last_vote_epoch: AtomicU64::new(0),
            failover_auth_time: AtomicU64::new(0),
            failover_auth_count: AtomicU64::new(0),
            failover_auth_rank: AtomicU64::new(0),
            failover_auth_epoch: AtomicU64::new(0),
        })
    }

    /// Loads the cluster state from a `nodes.conf` file.
    pub fn from_file(path: &str, server_config: &Config) -> Result<Self, SpinelDBError> {
        let content = std::fs::read_to_string(path)?;
        let mut s_state: SerializableClusterState =
            serde_json::from_str(&content).map_err(|e| SpinelDBError::Internal(e.to_string()))?;

        let slots_map = std::array::from_fn(|_| RwLock::new(None));
        let nodes = DashMap::new();

        for i in 0..s_state.nodes.len() {
            let mut node_info = s_state.nodes[i].clone();
            let mut pong_received = None;

            if node_info.get_flags().contains(NodeFlags::MYSELF) {
                let my_addr = server_config
                    .cluster
                    .announce_ip
                    .clone()
                    .unwrap_or_else(|| server_config.host.clone());
                let my_port = server_config
                    .cluster
                    .announce_port
                    .unwrap_or(server_config.port);

                let my_bus_port = match u32::from(server_config.port)
                    .checked_add(u32::from(server_config.cluster.bus_port_offset))
                {
                    Some(port_u32) if port_u32 <= u32::from(u16::MAX) => server_config
                        .cluster
                        .announce_bus_port
                        .unwrap_or(port_u32 as u16),
                    _ => {
                        return Err(SpinelDBError::Internal(
                            "Invalid cluster bus port configuration during load.".to_string(),
                        ));
                    }
                };

                node_info.addr = format!("{my_addr}:{my_port}");
                node_info.bus_addr = format!("{my_addr}:{my_bus_port}");
                pong_received = Some(Instant::now());

                if let Some(master_id) = &node_info.replica_of {
                    if let Some(master_node) = s_state.nodes.iter().find(|n| &n.id == master_id) {
                        if let Ok(mut config) = server_config.clone().into_mutex().try_lock() {
                            let parts: Vec<&str> = master_node.addr.split(':').collect();
                            if parts.len() == 2 {
                                let host = parts[0].to_string();
                                if let Ok(port) = parts[1].parse::<u16>() {
                                    info!(
                                        "Overriding replication config from nodes.conf: now replicating {}",
                                        master_node.addr
                                    );
                                    config.replication = ReplicationConfig::Replica {
                                        primary_host: host,
                                        primary_port: port,
                                        tls_enabled: false,
                                    };
                                }
                            }
                        }
                    }
                }

                s_state.nodes[i] = node_info.clone();
            }

            for &slot in &node_info.slots {
                *slots_map[slot as usize].write() = Some(node_info.id.clone());
            }

            let runtime_state = NodeRuntimeState {
                node_info: node_info.clone(),
                ping_sent: None,
                pong_received,
                pfail_reports: HashMap::new(),
            };
            nodes.insert(node_info.id, runtime_state);
        }

        Ok(Self {
            my_id: s_state.my_id,
            current_epoch: AtomicU64::new(s_state.current_epoch),
            last_purge_epoch: AtomicU64::new(0),
            nodes,
            slots_map,
            config_file_path: path.to_string(),
            last_vote_epoch: AtomicU64::new(0),
            failover_auth_time: AtomicU64::new(0),
            failover_auth_count: AtomicU64::new(0),
            failover_auth_rank: AtomicU64::new(0),
            failover_auth_epoch: AtomicU64::new(s_state.current_epoch),
        })
    }

    /// Saves the current cluster configuration to the `nodes.conf` file atomically.
    pub fn save_config(&self) -> Result<(), SpinelDBError> {
        let nodes_vec: Vec<ClusterNode> = self
            .nodes
            .iter()
            .map(|e| e.value().node_info.clone())
            .collect();

        let serializable = SerializableClusterState {
            my_id: self.my_id.clone(),
            current_epoch: self.current_epoch.load(Ordering::Relaxed),
            nodes: nodes_vec,
        };

        let content = serde_json::to_string_pretty(&serializable)
            .map_err(|e| SpinelDBError::Internal(e.to_string()))?;

        let temp_path = format!("{}.tmp-{}", self.config_file_path, rand::random::<u32>());
        std::fs::write(&temp_path, content)?;
        std::fs::rename(temp_path, &self.config_file_path)?;
        info!("Cluster config saved to {}", self.config_file_path);
        Ok(())
    }

    /// Records a PFAIL report from one node about another.
    pub fn mark_node_as_pfail(&self, node_id: &str, reporter_id: &str) {
        if let Some(mut runtime_state) = self.nodes.get_mut(node_id) {
            if !runtime_state
                .node_info
                .get_flags()
                .intersects(NodeFlags::MYSELF | NodeFlags::HANDSHAKE | NodeFlags::NOADDR)
            {
                let now = Instant::now();
                runtime_state
                    .pfail_reports
                    .insert(reporter_id.to_string(), now);
                info!("PFAIL report for {} from {}", node_id, reporter_id);
            }
        }
    }

    /// Marks a node as failed, usually based on a FAIL report from another node.
    pub fn mark_node_as_fail(&self, node_id: &str, reporter_id: &str) {
        if let Some(mut runtime_state) = self.nodes.get_mut(node_id) {
            let flags = runtime_state.node_info.get_flags();
            if flags.contains(NodeFlags::FAIL) || flags.contains(NodeFlags::MYSELF) {
                return;
            }

            info!(
                "Received FAIL report for node {} from {}. Marking as FAIL.",
                node_id, reporter_id
            );

            let mut new_flags = flags;
            new_flags.remove(NodeFlags::PFAIL);
            new_flags.insert(NodeFlags::FAIL);
            runtime_state.node_info.set_flags(new_flags);
        }
    }

    /// Cleans up old PFAIL reports to prevent them from persisting indefinitely.
    pub fn clean_pfail_reports(&self) {
        let timeout_ms = 15000;
        let timeout = Duration::from_millis(timeout_ms * 2);

        for mut entry in self.nodes.iter_mut() {
            entry
                .value_mut()
                .pfail_reports
                .retain(|_, &mut report_time| report_time.elapsed() < timeout);
        }
    }

    /// Promotes a node from PFAIL to FAIL if a majority of masters agree.
    pub fn promote_pfail_to_fail(&self, node_id: &str) -> bool {
        let needed = (self.count_online_masters() / 2) + 1;
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            if node.pfail_reports.len() >= needed {
                if node.node_info.get_flags().contains(NodeFlags::FAIL) {
                    return false;
                }
                info!("Marking node {} as FAIL", node_id);
                let mut flags = node.node_info.get_flags();
                flags.remove(NodeFlags::PFAIL);
                flags.insert(NodeFlags::FAIL);
                node.node_info.set_flags(flags);
                let _ = self.save_config();
                return true;
            }
        }
        false
    }

    /// Returns a reference to this node's own `NodeRuntimeState`.
    pub fn get_my_config(&self) -> Ref<String, NodeRuntimeState> {
        self.nodes
            .get(&self.my_id)
            .expect("Invariant violation: own node config should always exist in the cluster map")
    }

    /// Updates this node's role to PRIMARY after winning an election.
    pub fn update_my_role_to_master(&self, new_epoch: u64) {
        if let Some(mut myself) = self.nodes.get_mut(&self.my_id) {
            let mut flags = myself.node_info.get_flags();
            flags.remove(NodeFlags::REPLICA);
            flags.insert(NodeFlags::PRIMARY);
            myself.node_info.set_flags(flags);
            myself.node_info.replica_of = None;
            myself.node_info.config_epoch = new_epoch;
            info!(
                "Node {} promoted to PRIMARY for epoch {}",
                self.my_id, new_epoch
            );
        }
    }

    /// Takes over all hash slots from a failed master.
    pub fn take_over_slots_from(&self, old_master_id: &str) {
        let slots_to_claim: BTreeSet<u16> = if let Some(old_master) = self.nodes.get(old_master_id)
        {
            old_master.node_info.slots.clone()
        } else {
            return;
        };

        if slots_to_claim.is_empty() {
            return;
        }

        info!(
            "Taking over {} slots from old master {}",
            slots_to_claim.len(),
            old_master_id
        );

        if let Some(mut old_master_node) = self.nodes.get_mut(old_master_id) {
            old_master_node.node_info.slots.clear();
        }

        if let Some(mut myself) = self.nodes.get_mut(&self.my_id) {
            for slot in &slots_to_claim {
                *self.slots_map[*slot as usize].write() = Some(self.my_id.clone());
            }
            myself.node_info.slots.extend(slots_to_claim);
        }
    }

    /// Counts the number of masters currently considered to be online.
    pub fn count_online_masters(&self) -> usize {
        self.nodes
            .iter()
            .filter(|n| {
                let flags = n.value().node_info.get_flags();
                flags.contains(NodeFlags::PRIMARY)
                    && !flags.intersects(NodeFlags::FAIL | NodeFlags::PFAIL)
            })
            .count()
    }

    /// Generates a new, unique configuration epoch for this node.
    pub fn get_new_config_epoch(&self) -> u64 {
        let current = self.current_epoch.load(Ordering::Relaxed);
        let my_epoch = self.get_my_config().node_info.config_epoch;
        let new_epoch = current.max(my_epoch) + 1;
        self.current_epoch.store(new_epoch, Ordering::Relaxed);
        new_epoch
    }

    /// Returns a new, unique epoch for a cache purge operation.
    pub fn get_new_purge_epoch(&self) -> u64 {
        self.last_purge_epoch.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Merges information about another node received via gossip into our own state.
    pub async fn merge_node_info(&self, received_node: ClusterNode, state: &Arc<ServerState>) {
        if received_node.id == self.my_id {
            return;
        }
        if let Some(mut existing_runtime) = self.nodes.get_mut(&received_node.id) {
            let existing_node = &mut existing_runtime.node_info;
            if existing_node.config_epoch > received_node.config_epoch {
                return;
            }
            let pfail_flag = existing_node.get_flags() & NodeFlags::PFAIL;
            existing_node.set_flags(received_node.get_flags() | pfail_flag);
            existing_node.addr = received_node.addr.clone();
            existing_node.bus_addr = received_node.bus_addr.clone();
            existing_node.slots = received_node.slots.clone();
            existing_node.replica_of = received_node.replica_of.clone();
            existing_node.config_epoch = received_node.config_epoch;
            existing_node.replication_offset = received_node.replication_offset;
            existing_node.migrating_slots = received_node.migrating_slots.clone();
            existing_node.importing_slots = received_node.importing_slots.clone();
        } else {
            info!("Discovered new node {} via gossip", received_node.id);
            let new_runtime = NodeRuntimeState {
                node_info: received_node.clone(),
                ping_sent: None,
                pong_received: None,
                pfail_reports: HashMap::new(),
            };
            self.nodes
                .insert(new_runtime.node_info.id.clone(), new_runtime);
        }

        let my_config = self.get_my_config();
        if my_config.node_info.get_flags().contains(NodeFlags::PRIMARY)
            && received_node.get_flags().contains(NodeFlags::PRIMARY)
            && received_node.config_epoch > my_config.node_info.config_epoch
            && my_config
                .node_info
                .slots
                .iter()
                .any(|s| received_node.slots.contains(s))
        {
            let state_clone = state.clone();
            let self_clone = state.cluster.as_ref().unwrap().clone();
            tokio::spawn(async move {
                if let Err(e) = self_clone
                    .handle_epoch_conflict_and_reconfigure(state_clone, received_node)
                    .await
                {
                    error!(
                        "Failed to automatically reconfigure as replica after epoch conflict: {}",
                        e
                    );
                }
            });
        }
    }

    /// Handles the case where this node discovers a new primary with a higher epoch,
    /// triggering a self-demotion to a replica to prevent split-brain.
    async fn handle_epoch_conflict_and_reconfigure(
        &self,
        state: Arc<ServerState>,
        new_master_info: ClusterNode,
    ) -> Result<(), SpinelDBError> {
        info!(
            "CONFLICT: Detected new master {} with higher epoch {}. My epoch is {}. Stepping down to become a replica.",
            new_master_info.id,
            new_master_info.config_epoch,
            self.get_my_config().node_info.config_epoch,
        );

        {
            let mut config_guard = state.config.lock().await;
            let parts: Vec<&str> = new_master_info.addr.split(':').collect();
            if parts.len() != 2 {
                return Err(SpinelDBError::Internal(
                    "Invalid new master address format".into(),
                ));
            }
            let new_master_host = parts[0].to_string();
            let new_master_port = parts[1].parse::<u16>()?;

            config_guard.replication = ReplicationConfig::Replica {
                primary_host: new_master_host,
                primary_port: new_master_port,
                tls_enabled: false,
            };
        }

        if let Some(mut myself) = self.nodes.get_mut(&self.my_id) {
            let mut flags = myself.node_info.get_flags();
            flags.remove(NodeFlags::PRIMARY);
            flags.insert(NodeFlags::REPLICA);
            myself.node_info.set_flags(flags);
            myself.node_info.replica_of = Some(new_master_info.id.clone());
            myself.node_info.slots.clear();
        }

        self.save_config()?;

        state.set_quorum_loss_read_only(false, "Reconfiguring as a replica.");

        if state.replication_reconfigure_tx.send(()).is_err() {
            warn!(
                "Could not send reconfigure signal to replication worker; it may not be running or the channel is full."
            );
        }

        Ok(())
    }

    /// Checks if this node is the owner of a given slot.
    pub fn i_own_slot(&self, slot: u16) -> bool {
        self.slots_map[slot as usize]
            .read()
            .as_ref()
            .is_some_and(|id| *id == self.my_id)
    }

    /// Returns the node that is responsible for a given slot.
    pub fn get_node_for_slot(&self, slot: u16) -> Option<Ref<String, NodeRuntimeState>> {
        let owner_id = self.slots_map[slot as usize].read();
        let owner_id_str = owner_id.as_deref()?;
        self.nodes.get(owner_id_str)
    }
}
