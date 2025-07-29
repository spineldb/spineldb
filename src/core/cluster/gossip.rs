// src/core/cluster/gossip.rs

//! Implements the cluster gossip protocol for node discovery, state propagation,
//! and failure detection.

use crate::core::cluster::failover;
use crate::core::cluster::secure_gossip::SecureGossipMessage;
use crate::core::cluster::state::{ClusterNode, NodeFlags, NodeRuntimeState};
use crate::core::state::ServerState;
use bincode::config;
use bytes::Bytes;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tracing::{debug, error, info, warn};
use tracing_subscriber::filter::EnvFilter;

// Constants for the gossip protocol.
const GOSSIP_INTERVAL: Duration = Duration::from_secs(1);
const PROBE_INTERVAL: Duration = Duration::from_millis(100);
const GOSSIP_MAX_NODES_IN_PACKET: usize = 10;
const UDP_BUFFER_SIZE: usize = 65535;

/// The types of messages gossiped between nodes.
#[derive(Serialize, Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub enum GossipMessage {
    Meet {
        timestamp_ms: u64,
    },
    Ping {
        sender_id: String,
        gossip_nodes: Vec<ClusterNode>,
        timestamp_ms: u64,
    },
    Pong {
        sender_id: String,
        gossip_nodes: Vec<ClusterNode>,
        timestamp_ms: u64,
    },
    FailoverAuthRequest {
        sender_id: String,
        config_epoch: u64,
        replication_offset: u64,
        timestamp_ms: u64,
    },
    FailoverAuthAck {
        sender_id: String,
        config_epoch: u64,
        timestamp_ms: u64,
    },
    FailReport {
        sender_id: String,
        failed_node_id: String,
        timestamp_ms: u64,
    },
    Publish {
        sender_id: String,
        channel: Vec<u8>,
        message: Vec<u8>,
        timestamp_ms: u64,
    },
    PurgeTags {
        sender_id: String,
        tags_with_epoch: Vec<(Vec<u8>, u64)>, // Tuple of (tag, epoch)
        timestamp_ms: u64,
    },
    ConfigUpdate {
        sender_id: String,
        param: String,
        value: String,
        timestamp_ms: u64,
    },
}

impl GossipMessage {
    /// Returns the timestamp of the gossip message.
    pub fn timestamp(&self) -> u64 {
        match self {
            GossipMessage::Meet { timestamp_ms }
            | GossipMessage::Ping { timestamp_ms, .. }
            | GossipMessage::Pong { timestamp_ms, .. }
            | GossipMessage::FailoverAuthRequest { timestamp_ms, .. }
            | GossipMessage::FailoverAuthAck { timestamp_ms, .. }
            | GossipMessage::FailReport { timestamp_ms, .. }
            | GossipMessage::Publish { timestamp_ms, .. }
            | GossipMessage::PurgeTags { timestamp_ms, .. }
            | GossipMessage::ConfigUpdate { timestamp_ms, .. } => *timestamp_ms,
        }
    }
}

/// Messages sent from command handlers to the gossip task.
#[derive(Debug)]
pub enum GossipTaskMessage {
    /// Broadcast a message to all known nodes (used for PUBLISH).
    Broadcast(GossipMessage),
    /// Send a message to a specific target (used for CLUSTER MEET).
    DirectSend {
        message: GossipMessage,
        target: SocketAddr,
    },
}

/// Helper to get the current system time in milliseconds since the UNIX epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The main entry point for the gossip background task.
pub async fn run(
    state: Arc<ServerState>,
    bus_port: u16,
    mut shutdown_rx: broadcast::Receiver<()>,
    mut gossip_task_rx: mpsc::Receiver<GossipTaskMessage>,
) {
    let addr = format!("0.0.0.0:{bus_port}");
    let socket = match UdpSocket::bind(&addr).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            error!("Failed to bind cluster bus on UDP port {}: {}", bus_port, e);
            return;
        }
    };
    info!("Cluster bus listening on UDP port {}.", bus_port);

    // Task for receiving messages from other nodes.
    let receiver_state = state.clone();
    let receiver_socket = socket.clone();
    tokio::spawn(async move {
        let mut buf = [0; UDP_BUFFER_SIZE];
        loop {
            match receiver_socket.recv_from(&mut buf).await {
                Ok((len, src)) => {
                    let (password, node_timeout) = {
                        let config = receiver_state.config.lock().await;
                        (config.password.clone(), config.cluster.node_timeout)
                    };
                    let bincode_config = config::standard();
                    match bincode::decode_from_slice::<SecureGossipMessage, _>(
                        &buf[..len],
                        bincode_config,
                    ) {
                        Ok((secure_msg, _)) => match secure_msg.verify(&password) {
                            Ok(true) => {
                                handle_gossip_message(
                                    &receiver_state,
                                    secure_msg.message,
                                    &receiver_socket,
                                    src,
                                    node_timeout,
                                )
                                .await
                            }
                            Ok(false) => {
                                warn!(
                                    "Received gossip message with invalid signature from {}. Ignoring.",
                                    src
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Error verifying gossip message signature from {}: {}",
                                    src, e
                                );
                            }
                        },
                        Err(e) => warn!(
                            "Failed to deserialize secure gossip message from {}: {}",
                            src, e
                        ),
                    }
                }
                Err(e) => error!("Error receiving from cluster bus: {}", e),
            }
        }
    });

    let mut gossip_tick = time::interval(GOSSIP_INTERVAL);
    let mut probe_tick = time::interval(PROBE_INTERVAL);

    // Main loop for the gossip worker.
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => { info!("Gossip worker shutting down."); return; }
            _ = gossip_tick.tick() => {
                send_pings(&state, &socket).await;
            }
            _ = probe_tick.tick() => {
                 check_for_failed_nodes(&state, &socket).await;
                 check_quorum_and_self_fence(&state).await;
                 failover::handle_failover_cron(&state, &socket).await;
            }
            Some(task_message) = gossip_task_rx.recv() => {
                match task_message {
                    GossipTaskMessage::Broadcast(message) => {
                        broadcast_gossip_message(&state, &socket, message).await;
                    }
                    GossipTaskMessage::DirectSend { message, target } => {
                        send_direct_gossip_message(&state, &socket, message, target).await;
                    }
                }
            }
        }
    }
}

/// Centralized helper to send a message to a single specific target.
async fn send_direct_gossip_message(
    state: &Arc<ServerState>,
    socket: &Arc<UdpSocket>,
    message: GossipMessage,
    target: SocketAddr,
) {
    let password = &state.config.lock().await.password;
    if let Ok(secure_message) = SecureGossipMessage::new(message, password) {
        let bincode_config = config::standard();
        if let Ok(encoded_msg) = bincode::encode_to_vec(&secure_message, bincode_config) {
            if let Err(e) = socket.send_to(&encoded_msg, &target).await {
                warn!("Failed to send direct gossip message to {}: {}", target, e);
            }
        }
    }
}

/// Centralized helper to broadcast a gossip message to all other nodes in the cluster.
async fn broadcast_gossip_message(
    state: &Arc<ServerState>,
    socket: &Arc<UdpSocket>,
    message: GossipMessage,
) {
    let cluster = state.cluster.as_ref().unwrap();
    let password = &state.config.lock().await.password;
    if let Ok(secure_message) = SecureGossipMessage::new(message, password) {
        let bincode_config = config::standard();
        if let Ok(encoded_msg) = bincode::encode_to_vec(&secure_message, bincode_config) {
            for entry in cluster.nodes.iter() {
                let node_info = &entry.value().node_info;
                if !node_info
                    .get_flags()
                    .intersects(NodeFlags::MYSELF | NodeFlags::FAIL | NodeFlags::HANDSHAKE)
                {
                    if let Err(e) = socket.send_to(&encoded_msg, &node_info.bus_addr).await {
                        warn!(
                            "Failed to send broadcast gossip message to {}: {}",
                            node_info.bus_addr, e
                        );
                    }
                }
            }
        }
    }
}

fn choose_nodes_to_ping(state: &Arc<ServerState>) -> Vec<NodeRuntimeState> {
    let cluster = state.cluster.as_ref().unwrap();
    let nodes: Vec<_> = cluster
        .nodes
        .iter()
        .filter(|node| {
            node.key() != &cluster.my_id
                && !node
                    .value()
                    .node_info
                    .get_flags()
                    .contains(NodeFlags::HANDSHAKE)
        })
        .map(|node| node.value().clone())
        .collect();

    if nodes.is_empty() {
        return vec![];
    }

    let mut rng = rand::thread_rng();
    let sample_size = (nodes.len() / 2).max(1);
    nodes
        .choose_multiple(&mut rng, sample_size)
        .cloned()
        .collect()
}

async fn send_pings(state: &Arc<ServerState>, socket: &Arc<UdpSocket>) {
    let cluster = state.cluster.as_ref().unwrap();
    let password = &state.config.lock().await.password;

    let replica_info = state.replication.replica_info.lock().await;
    if let Some(info) = replica_info.as_ref() {
        if let Some(mut myself) = cluster.nodes.get_mut(&cluster.my_id) {
            myself.value_mut().node_info.replication_offset = info.processed_offset;
        }
    }
    drop(replica_info);

    let chosen_nodes = choose_nodes_to_ping(state);
    for mut runtime_state in chosen_nodes {
        let node_info = &runtime_state.node_info;
        let gossip_nodes = select_nodes_for_gossip(state);
        let ping_msg = GossipMessage::Ping {
            sender_id: cluster.my_id.clone(),
            gossip_nodes,
            timestamp_ms: now_ms(),
        };

        if let Ok(secure_ping_msg) = SecureGossipMessage::new(ping_msg, password) {
            let bincode_config = config::standard();
            if let Ok(encoded) = bincode::encode_to_vec(&secure_ping_msg, bincode_config) {
                if let Err(e) = socket.send_to(&encoded, &node_info.bus_addr).await {
                    error!("Failed to send PING to {}: {}", node_info.bus_addr, e);
                } else {
                    debug!("Sent PING to {}", node_info.bus_addr);
                    runtime_state.ping_sent = Some(Instant::now());
                    cluster.nodes.insert(node_info.id.clone(), runtime_state);
                }
            } else {
                error!("Failed to serialize secure PING message");
            }
        } else {
            error!("Failed to sign PING message");
        }
    }
}

async fn check_for_failed_nodes(state: &Arc<ServerState>, socket: &Arc<UdpSocket>) {
    let cluster = state.cluster.as_ref().unwrap();
    let (node_timeout, password) = {
        let config_guard = state.config.lock().await;
        (
            Duration::from_millis(config_guard.cluster.node_timeout),
            config_guard.password.clone(),
        )
    };

    cluster.clean_pfail_reports();

    for mut entry in cluster.nodes.iter_mut() {
        let node_id = entry.key().clone();
        let runtime_state = entry.value_mut();

        let flags = runtime_state.node_info.get_flags();
        if flags.intersects(NodeFlags::MYSELF | NodeFlags::HANDSHAKE | NodeFlags::FAIL) {
            continue;
        }

        if let Some(pong_time) = runtime_state.pong_received {
            if pong_time.elapsed() > node_timeout && !flags.contains(NodeFlags::PFAIL) {
                info!("Marking node {} as PFAIL (no PONG received)", node_id);
                let mut new_flags = flags;
                new_flags.insert(NodeFlags::PFAIL);
                runtime_state.node_info.set_flags(new_flags);
            }
        }

        if flags.contains(NodeFlags::PFAIL) && cluster.promote_pfail_to_fail(&node_id).await {
            info!("Broadcasting FAIL report for node {}", node_id);
            let fail_report_msg = GossipMessage::FailReport {
                sender_id: cluster.my_id.clone(),
                failed_node_id: node_id.clone(),
                timestamp_ms: now_ms(),
            };

            if let Ok(secure_report) = SecureGossipMessage::new(fail_report_msg, &password) {
                let bincode_config = config::standard();
                if let Ok(encoded_msg) = bincode::encode_to_vec(&secure_report, bincode_config) {
                    for other_node_entry in cluster.nodes.iter() {
                        let other_node_id = other_node_entry.key();
                        let other_node_info = &other_node_entry.value().node_info;
                        if other_node_id != &cluster.my_id && other_node_id != &node_id {
                            let _ = socket
                                .send_to(&encoded_msg, &other_node_info.bus_addr)
                                .await;
                        }
                    }
                }
            }
        }
    }
}

/// Periodically checks if this master node can still see a quorum of other masters.
/// If not, it puts itself into a read-only state to prevent split-brain.
async fn check_quorum_and_self_fence(state: &Arc<ServerState>) {
    let Some(cluster) = state.cluster.as_ref() else {
        return;
    };

    if !cluster
        .get_my_config()
        .node_info
        .get_flags()
        .contains(NodeFlags::PRIMARY)
    {
        if state
            .is_read_only_due_to_quorum_loss
            .load(Ordering::Relaxed)
        {
            state.set_quorum_loss_read_only(false, "Node is now a replica.");
        }
        return;
    }

    let quorum = state.config.lock().await.cluster.failover_quorum;
    let online_masters = cluster.count_online_masters();
    let currently_fenced = state
        .is_read_only_due_to_quorum_loss
        .load(Ordering::Relaxed);

    if online_masters < quorum && !currently_fenced {
        let reason = format!(
            "Lost contact with cluster majority. Can only see {online_masters}/{quorum} masters."
        );
        state.set_quorum_loss_read_only(true, &reason);
    } else if online_masters >= quorum && currently_fenced {
        let reason = format!(
            "Re-established contact with cluster majority. Can see {online_masters}/{quorum} masters."
        );
        state.set_quorum_loss_read_only(false, &reason);
    }
}

async fn handle_gossip_message(
    state: &Arc<ServerState>,
    msg: GossipMessage,
    socket: &Arc<UdpSocket>,
    src_addr: std::net::SocketAddr,
    node_timeout: u64,
) {
    let cluster = state.cluster.as_ref().unwrap();
    let password = &state.config.lock().await.password;
    let time_window = Duration::from_millis(node_timeout * 2).as_millis();
    let now = now_ms();
    let msg_ts = msg.timestamp();

    if now.saturating_sub(msg_ts) as u128 > time_window
        || msg_ts.saturating_sub(now) as u128 > time_window
    {
        warn!(
            "Dropping stale gossip message from {}: message ts={}, now={}, diff={}ms",
            src_addr,
            msg_ts,
            now,
            (now as i64 - msg_ts as i64).abs()
        );
        return;
    }

    debug!(
        "Handling verified gossip message: {:?} from {}",
        msg, src_addr
    );

    match msg {
        GossipMessage::Meet { .. } => {
            let gossip_nodes = select_nodes_for_gossip(state);
            let ping_msg = GossipMessage::Ping {
                sender_id: cluster.my_id.clone(),
                gossip_nodes,
                timestamp_ms: now_ms(),
            };
            if let Ok(secure_msg) = SecureGossipMessage::new(ping_msg, password) {
                let bincode_config = config::standard();
                if let Ok(encoded) = bincode::encode_to_vec(&secure_msg, bincode_config) {
                    let _ = socket.send_to(&encoded, &src_addr).await;
                }
            }
        }
        GossipMessage::Ping {
            sender_id,
            gossip_nodes,
            ..
        } => {
            if let Some(sender_runtime) = cluster.nodes.get(&sender_id) {
                for received_node_info in gossip_nodes {
                    cluster.merge_node_info(received_node_info, state).await;
                }
                let my_gossip_nodes = select_nodes_for_gossip(state);
                let pong_msg = GossipMessage::Pong {
                    sender_id: cluster.my_id.clone(),
                    gossip_nodes: my_gossip_nodes,
                    timestamp_ms: now_ms(),
                };
                if let Ok(secure_pong) = SecureGossipMessage::new(pong_msg, password) {
                    let bincode_config = config::standard();
                    if let Ok(encoded) = bincode::encode_to_vec(&secure_pong, bincode_config) {
                        let _ = socket
                            .send_to(&encoded, &sender_runtime.node_info.bus_addr)
                            .await;
                    }
                }
            } else {
                warn!(
                    "Received PING from unknown node ID {}. Responding with MEET logic.",
                    sender_id
                );
                let my_gossip_nodes = select_nodes_for_gossip(state);
                let ping_msg = GossipMessage::Ping {
                    sender_id: cluster.my_id.clone(),
                    gossip_nodes: my_gossip_nodes,
                    timestamp_ms: now_ms(),
                };
                if let Ok(secure_msg) = SecureGossipMessage::new(ping_msg, password) {
                    let bincode_config = config::standard();
                    if let Ok(encoded) = bincode::encode_to_vec(&secure_msg, bincode_config) {
                        let _ = socket.send_to(&encoded, &src_addr).await;
                    }
                }
            }
        }
        GossipMessage::Pong {
            sender_id,
            gossip_nodes,
            ..
        } => {
            if let Some(mut sender_runtime) = cluster.nodes.get_mut(&sender_id) {
                sender_runtime.pong_received = Some(Instant::now());
                if sender_runtime
                    .node_info
                    .get_flags()
                    .contains(NodeFlags::PFAIL)
                {
                    info!("Node {} is back online. Removing PFAIL flag.", sender_id);
                    let mut flags = sender_runtime.node_info.get_flags();
                    flags.remove(NodeFlags::PFAIL);
                    sender_runtime.node_info.set_flags(flags);
                }
            }
            for received_node_info in gossip_nodes {
                cluster.merge_node_info(received_node_info, state).await;
            }
        }
        GossipMessage::FailoverAuthRequest {
            sender_id,
            config_epoch,
            replication_offset,
            ..
        } => {
            failover::handle_auth_request(
                state,
                socket,
                sender_id,
                config_epoch,
                replication_offset,
            )
            .await;
        }
        GossipMessage::FailoverAuthAck {
            sender_id,
            config_epoch,
            ..
        } => {
            failover::handle_auth_ack(state, sender_id, config_epoch).await;
        }
        GossipMessage::FailReport {
            sender_id,
            failed_node_id,
            ..
        } => {
            cluster.mark_node_as_fail(&failed_node_id, &sender_id);
        }
        GossipMessage::Publish {
            sender_id,
            channel,
            message,
            ..
        } => {
            if cluster.my_id == sender_id {
                return;
            }
            debug!(
                "Received forwarded PUBLISH for channel '{}' from node {}",
                String::from_utf8_lossy(&channel),
                sender_id
            );
            let channel_bytes = Bytes::from(channel);
            let message_bytes = Bytes::from(message);
            state.pubsub.publish(&channel_bytes, message_bytes);
        }
        GossipMessage::PurgeTags {
            sender_id,
            tags_with_epoch,
            ..
        } => {
            if cluster.my_id == sender_id {
                return;
            }

            debug!("Received forwarded PURGETAG from node {}", sender_id);

            for (tag_bytes, epoch) in tags_with_epoch {
                let tag = Bytes::from(tag_bytes);
                state
                    .cache
                    .tag_purge_epochs
                    .entry(tag)
                    .and_modify(|e| {
                        if epoch > *e {
                            *e = epoch;
                        }
                    })
                    .or_insert(epoch);
            }
        }
        GossipMessage::ConfigUpdate {
            sender_id,
            param,
            value,
            ..
        } => {
            if cluster.my_id == sender_id {
                return;
            }
            info!("Received CONFIG SET {param} {value} from node {sender_id}. Applying locally.");
            let mut config = state.config.lock().await;
            match param.to_lowercase().as_str() {
                "maxmemory" => {
                    if let Ok(bytes) = value.parse() {
                        config.maxmemory = if bytes == 0 { None } else { Some(bytes) };
                    }
                }
                "loglevel" => {
                    if let Ok(new_filter) = EnvFilter::try_new(&value) {
                        if let Err(e) = state.log_reload_handle.reload(new_filter) {
                            warn!("Failed to apply propagated log level change: {e}");
                        } else {
                            config.log_level = value;
                        }
                    }
                }
                _ => {
                    warn!("Received unknown CONFIG SET parameter '{param}' from gossip. Ignoring.");
                }
            }
        }
    }
}

fn select_nodes_for_gossip(state: &Arc<ServerState>) -> Vec<ClusterNode> {
    let cluster = state.cluster.as_ref().unwrap();
    let mut nodes_to_gossip: Vec<ClusterNode> = cluster
        .nodes
        .iter()
        .map(|entry| entry.value().node_info.clone())
        .collect();

    let mut rng = rand::thread_rng();
    nodes_to_gossip.shuffle(&mut rng);
    nodes_to_gossip.truncate(GOSSIP_MAX_NODES_IN_PACKET);
    nodes_to_gossip
}
