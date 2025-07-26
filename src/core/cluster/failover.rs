// src/core/cluster/failover.rs

//! Implements the replica-initiated failover logic for the cluster.
//! This includes master failure detection, election requests, and voting.
//!
//! # WARNING: Risk of Split-Brain and Data Inconsistency
//!
//! This replica-initiated failover mechanism is provided for basic high-availability
//! but is susceptible to "split-brain" scenarios during network partitions. If a
//! partition isolates the master from the majority of the cluster, the majority
//! may elect a new master while the old one continues to accept writes from a
//! minority of clients. This leads to permanent data inconsistency.
//!
//! **For production environments, it is strongly recommended to use SpinelDB's
//! Warden mode (`--warden`) instead.** Warden acts as an external sentinel,
//! providing a more robust quorum and fencing mechanism to prevent split-brain.
//!
//! # Operational Requirements: Time Synchronization
//!
//! The failover mechanism relies on a monotonically increasing configuration epoch
//! (`config_epoch`) to ensure that votes are cast for the most current election.
//! While the system has measures to prevent rapid, successive elections, it
//! fundamentally assumes that the system clocks (wall clocks) across all nodes
//! in the cluster are reasonably synchronized.
//!
//! It is **strongly recommended** to use a time synchronization service like NTP
//! (Network Time Protocol) on all nodes running SpinelDB in cluster mode.
//! Significant clock skew between nodes could potentially lead to premature or
//! unnecessary failover events.

use crate::config::{ReplicationConfig, ReplicationPrimaryConfig};
use crate::core::cluster::gossip::{GossipMessage, now_ms};
use crate::core::cluster::secure_gossip::SecureGossipMessage;
use crate::core::cluster::state::NodeFlags;
use crate::core::state::ServerState;
use bincode::config;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::UdpSocket;
use tracing::{info, warn};

/// The base delay before a replica initiates a failover election.
/// A random delay is added to this to prevent multiple replicas from starting an election simultaneously.
const FAILOVER_BASE_DELAY_MS: u64 = 500;

/// This function is called periodically by the cluster cron job (`probe_tick` in gossip.rs).
/// It checks if this node is a replica and if its master is in a failure state.
pub async fn handle_failover_cron(state: &Arc<ServerState>, socket: &Arc<UdpSocket>) {
    // Check if the replica-initiated failover feature is enabled in the configuration.
    // If not, this function does nothing, making Warden the only failover mechanism.
    if !state.config.lock().await.cluster.replica_initiated_failover {
        return;
    }

    let cluster = state
        .cluster
        .as_ref()
        .expect("Failover cron must run in cluster mode");

    if let Some(my_master_id) = &cluster.get_my_config().node_info.replica_of {
        if let Some(master_node) = cluster.nodes.get(my_master_id) {
            if master_node
                .node_info
                .get_flags()
                .intersects(NodeFlags::FAIL | NodeFlags::PFAIL)
            {
                start_election(state, socket).await;
            }
        }
    }
}

/// Starts the election process for this replica to become a new master.
async fn start_election(state: &Arc<ServerState>, socket: &Arc<UdpSocket>) {
    let cluster = state
        .cluster
        .as_ref()
        .expect("start_election must run in cluster mode");
    let now_unix_ms = now_ms();

    let last_auth_time = cluster.failover_auth_time.load(Ordering::Relaxed);
    if now_unix_ms < last_auth_time + (FAILOVER_BASE_DELAY_MS * 2) {
        return;
    }

    if let Some(my_master_id) = &cluster.get_my_config().node_info.replica_of {
        let my_offset = state
            .replication
            .replica_info
            .lock()
            .await
            .as_ref()
            .map_or(0, |i| i.processed_offset);

        for entry in cluster.nodes.iter() {
            let other_node = &entry.value().node_info;
            if other_node.id != cluster.my_id
                && other_node.replica_of.as_deref() == Some(my_master_id)
                && other_node.replication_offset > my_offset
            {
                info!(
                    "Aborting election. Node {} has a higher replication offset ({} vs my {}).",
                    other_node.id, other_node.replication_offset, my_offset
                );
                return;
            }
        }
    }

    let random_delay_ms = rand::thread_rng().gen_range(0..=FAILOVER_BASE_DELAY_MS);
    let total_delay = Duration::from_millis(FAILOVER_BASE_DELAY_MS + random_delay_ms);
    info!(
        "Master is down. Waiting {:?} before starting election.",
        total_delay
    );
    tokio::time::sleep(total_delay).await;

    if let Some(my_master_id) = &cluster.get_my_config().node_info.replica_of {
        if let Some(master_node) = cluster.nodes.get(my_master_id) {
            if !master_node
                .node_info
                .get_flags()
                .intersects(NodeFlags::FAIL | NodeFlags::PFAIL)
            {
                info!("Master is back online. Aborting election.");
                return;
            }
        }
    }

    let new_epoch = cluster.get_new_config_epoch();
    cluster
        .failover_auth_time
        .store(now_unix_ms, Ordering::Relaxed);
    cluster.failover_auth_count.store(1, Ordering::Relaxed); // Count own vote.
    cluster
        .failover_auth_epoch
        .store(new_epoch, Ordering::Relaxed);

    let my_offset = state
        .replication
        .replica_info
        .lock()
        .await
        .as_ref()
        .map_or(0, |info| info.processed_offset);

    info!(
        "Starting a new election for epoch {} with offset {}",
        new_epoch, my_offset
    );

    let password = &state.config.lock().await.password;

    let auth_request = GossipMessage::FailoverAuthRequest {
        sender_id: cluster.my_id.clone(),
        config_epoch: new_epoch,
        replication_offset: my_offset,
        timestamp_ms: now_ms(),
    };

    if let Ok(secure_request) = SecureGossipMessage::new(auth_request, password) {
        let bincode_config = config::standard();
        if let Ok(encoded_msg) = bincode::encode_to_vec(&secure_request, bincode_config) {
            for entry in cluster.nodes.iter() {
                let node = &entry.value().node_info;
                if node.get_flags().contains(NodeFlags::PRIMARY)
                    && !node.get_flags().contains(NodeFlags::MYSELF)
                {
                    let _ = socket.send_to(&encoded_msg, &node.bus_addr).await;
                }
            }
        }
    }
}

/// Handles a vote request from another replica that is running for election.
pub async fn handle_auth_request(
    state: &Arc<ServerState>,
    socket: &Arc<UdpSocket>,
    candidate_id: String,
    candidate_epoch: u64,
    candidate_offset: u64,
) {
    let cluster = state
        .cluster
        .as_ref()
        .expect("handle_auth_request must run in cluster mode");
    if !cluster
        .get_my_config()
        .node_info
        .get_flags()
        .contains(NodeFlags::PRIMARY)
    {
        return;
    }

    let last_vote_epoch = cluster.last_vote_epoch.load(Ordering::Relaxed);
    if candidate_epoch > last_vote_epoch {
        if let Some(candidate_node_entry) = cluster.nodes.get(&candidate_id) {
            if let Some(failed_master_id) = &candidate_node_entry.node_info.replica_of {
                if let Some(failed_master_entry) = cluster.nodes.get(failed_master_id) {
                    let last_known_master_offset = failed_master_entry.node_info.replication_offset;
                    if candidate_offset < last_known_master_offset {
                        warn!(
                            "Rejecting vote for {}: candidate offset ({}) is older than last known master offset ({}).",
                            candidate_id, candidate_offset, last_known_master_offset
                        );
                        return;
                    }
                }
            }
        }

        cluster
            .last_vote_epoch
            .store(candidate_epoch, Ordering::Relaxed);
        info!(
            "Voting for node {} in epoch {}",
            candidate_id, candidate_epoch
        );

        let password = &state.config.lock().await.password;

        let ack_msg = GossipMessage::FailoverAuthAck {
            sender_id: cluster.my_id.clone(),
            config_epoch: candidate_epoch,
            timestamp_ms: now_ms(),
        };

        if let Ok(secure_ack) = SecureGossipMessage::new(ack_msg, password) {
            let bincode_config = config::standard();
            if let Some(candidate_node) = cluster.nodes.get(&candidate_id) {
                if let Ok(encoded) = bincode::encode_to_vec(&secure_ack, bincode_config) {
                    let _ = socket
                        .send_to(&encoded, &candidate_node.node_info.bus_addr)
                        .await;
                }
            }
        }
    } else {
        warn!(
            "Received stale vote request from {} for epoch {} (last vote was for epoch {})",
            candidate_id, candidate_epoch, last_vote_epoch
        );
    }
}

/// Handles a vote reply (FailoverAuthAck) from a master.
pub async fn handle_auth_ack(state: &Arc<ServerState>, sender_id: String, ack_epoch: u64) {
    let cluster = state
        .cluster
        .as_ref()
        .expect("handle_auth_ack must run in cluster mode");
    let my_election_epoch = cluster.failover_auth_epoch.load(Ordering::Relaxed);

    if !cluster
        .get_my_config()
        .node_info
        .get_flags()
        .contains(NodeFlags::REPLICA)
        || ack_epoch != my_election_epoch
    {
        return;
    }

    let current_votes = cluster.failover_auth_count.fetch_add(1, Ordering::Relaxed) + 1;
    info!(
        "Received vote from {}. Total votes: {}",
        sender_id, current_votes
    );

    let needed_votes = (cluster.count_online_masters() / 2) + 1;
    if current_votes >= needed_votes as u64 {
        info!(
            "Won the election with {} votes. Promoting to master.",
            current_votes
        );
        promote_to_master(state).await;
        cluster.failover_auth_count.store(0, Ordering::Relaxed);
    }
}

/// Promotes this node from a replica to a master after winning an election.
async fn promote_to_master(state: &Arc<ServerState>) {
    let cluster = state
        .cluster
        .as_ref()
        .expect("promote_to_master must run in cluster mode");
    let my_old_master_id = cluster.get_my_config().node_info.replica_of.clone();

    let election_epoch = cluster.failover_auth_epoch.load(Ordering::Relaxed);
    cluster.update_my_role_to_master(election_epoch);

    if let Some(old_master_id) = my_old_master_id {
        cluster.take_over_slots_from(&old_master_id);
    }

    // Update the main server configuration to reflect the new role.
    // This is critical for the server to start its replication backlog feeder
    // and accept PSYNC requests from other replicas.
    {
        let mut config = state.config.lock().await;
        config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());
        info!("Updated main server config to PRIMARY role.");
    }

    // Signal the replication worker to reconfigure.
    // The worker should see the new config and terminate, allowing the main
    // server task spawner to start the backlog feeder.
    if state.replication_reconfigure_tx.send(()).is_err() {
        warn!("Could not send reconfigure signal to replication worker after promotion.");
    }

    let _ = cluster.save_config();
}
