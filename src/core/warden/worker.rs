// src/core/warden/worker.rs

//! Implements the main monitoring and failover logic for a single master.

use super::client::WardenClient;
use super::failover;
use super::state::{
    FailoverState, GlobalWardenState, InstanceState, MasterState, MasterStatus, WardenPeerState,
};
use crate::core::protocol::RespFrame;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, error, info, warn};

/// The Pub/Sub channel used by Wardens to announce their presence.
pub(super) const HELLO_CHANNEL: &str = "__warden__:hello";
/// The Pub/Sub channel used for failover voting.
pub(super) const VOTES_CHANNEL: &str = "__warden__:votes";

/// The payload of a `HELLO` message broadcast by a Warden instance.
#[derive(Serialize, Deserialize, Debug)]
struct HelloMessage {
    addr: SocketAddr,
    run_id: String,
    epoch: u64,
    master_name: String,
    master_addr: SocketAddr,
}

/// A cloneable struct that runs the monitoring loops for a single master.
#[derive(Clone)]
pub struct MasterMonitor {
    master_name: String,
    state: Arc<Mutex<MasterState>>,
    global_state: Arc<GlobalWardenState>,
    my_announce_addr: SocketAddr,
}

impl MasterMonitor {
    /// Creates a new `MasterMonitor`.
    pub fn new(
        master_name: String,
        state: Arc<Mutex<MasterState>>,
        global_state: Arc<GlobalWardenState>,
        my_announce_addr: SocketAddr,
    ) -> Self {
        Self {
            master_name,
            state,
            global_state,
            my_announce_addr,
        }
    }

    pub fn master_name(&self) -> &str {
        &self.master_name
    }

    /// The main entry point for the monitor, which spawns its sub-tasks.
    pub async fn run(&self) {
        let (tick_interval, info_interval, hello_interval) = {
            let state = self.state.lock();
            let down_after = state.config.down_after;
            let tick_interval = (down_after / 3).max(Duration::from_secs(1));
            let info_interval = (down_after * 2).max(Duration::from_secs(10));
            let hello_interval = state.config.hello_interval;
            (tick_interval, info_interval, hello_interval)
        };

        info!(
            "Monitor for '{}' started with health check interval {:?}, info poll interval {:?}, and hello interval {:?}.",
            self.master_name, tick_interval, info_interval, hello_interval
        );

        let mut tasks = tokio::task::JoinSet::new();

        tasks.spawn(self.clone().run_tick_loop(tick_interval));
        tasks.spawn(self.clone().run_info_loop(info_interval));
        tasks.spawn(self.clone().run_pubsub_loop(hello_interval));

        if let Some(res) = tasks.join_next().await {
            error!(
                "A sub-task for monitor '{}' exited unexpectedly: {:?}",
                self.master_name, res
            );
        }
    }

    /// The main periodic timer for health checks and state transitions.
    async fn run_tick_loop(self, interval: Duration) {
        let mut tick_timer = time::interval(interval);
        loop {
            tick_timer.tick().await;
            self.check_master_down().await;
            self.check_replicas_down().await;
            self.reconfigure_stale_replicas().await;
            self.check_failover_status().await;
            self.check_election_status().await;
        }
    }

    /// The periodic timer for polling `INFO REPLICATION` from the master.
    async fn run_info_loop(self, interval: Duration) {
        let mut info_timer = time::interval(interval);
        loop {
            info_timer.tick().await;
            self.poll_master_info().await;
        }
    }

    /// The main loop for managing the Pub/Sub connection for inter-Warden communication.
    async fn run_pubsub_loop(self, hello_interval: Duration) {
        const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
        const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
        let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

        loop {
            let master_addr = self.state.lock().addr;

            let pubsub_client = match self.connect_and_subscribe(master_addr).await {
                Ok(client) => {
                    info!(
                        "Successfully subscribed to channels on master '{}' at {}",
                        self.master_name, master_addr
                    );
                    reconnect_delay = INITIAL_RECONNECT_DELAY; // Reset delay on success.
                    Some(client)
                }
                Err(e) => {
                    warn!(
                        "Failed to connect or subscribe for master '{}': {}. Retrying in {:?}...",
                        self.master_name, e, reconnect_delay
                    );
                    None
                }
            };

            if let Some(client) = pubsub_client {
                if let Err(e) = self.process_pubsub_messages(client, hello_interval).await {
                    warn!(
                        "Pub/Sub connection for '{}' lost: {}. Reconnecting...",
                        self.master_name, e
                    );
                }
            }

            time::sleep(reconnect_delay).await;
            // Apply exponential backoff for subsequent reconnection attempts.
            reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
        }
    }

    /// Establishes a connection to the master and subscribes to Warden channels.
    async fn connect_and_subscribe(&self, master_addr: SocketAddr) -> anyhow::Result<WardenClient> {
        let mut client = WardenClient::connect(master_addr).await?;
        let cmd = RespFrame::Array(vec![
            RespFrame::BulkString("SUBSCRIBE".into()),
            RespFrame::BulkString(HELLO_CHANNEL.into()),
            RespFrame::BulkString(VOTES_CHANNEL.into()),
        ]);

        // Expect 3 successful subscription confirmations.
        for _ in 0..3 {
            client.send_and_receive(cmd.clone()).await?;
        }
        Ok(client)
    }

    /// Processes incoming Pub/Sub messages and periodically publishes hello messages.
    async fn process_pubsub_messages(
        &self,
        mut client: WardenClient,
        hello_interval: Duration,
    ) -> anyhow::Result<()> {
        let mut hello_timer = time::interval(hello_interval);

        loop {
            tokio::select! {
                _ = hello_timer.tick() => {
                    self.publish_hello_message().await?;
                }
                result = client.send_and_receive(RespFrame::Array(vec![])) => {
                     let frame = result?;
                     if let RespFrame::Array(parts) = frame {
                        if parts.len() == 3 {
                           if let (RespFrame::BulkString(channel), RespFrame::BulkString(payload)) = (&parts[1], &parts[2]) {
                                self.process_management_message(channel, payload).await;
                           }
                        }
                     }
                }
            }
        }
    }

    /// Dispatches a Pub/Sub message to the appropriate handler.
    async fn process_management_message(&self, channel: &Bytes, payload: &Bytes) {
        let channel_str = String::from_utf8_lossy(channel);
        if channel_str == HELLO_CHANNEL {
            self.process_hello_message(payload);
        } else if channel_str == VOTES_CHANNEL {
            self.process_vote_message(payload).await;
        }
    }

    /// Publishes a message to a specific channel on the master.
    async fn publish_message(&self, channel: String, message: String) -> anyhow::Result<()> {
        let master_addr = self.state.lock().addr;
        let mut client = WardenClient::connect(master_addr).await?;
        let cmd = RespFrame::Array(vec![
            RespFrame::BulkString("PUBLISH".into()),
            RespFrame::BulkString(channel.into()),
            RespFrame::BulkString(message.into()),
        ]);
        client.send_and_receive(cmd).await?;
        Ok(())
    }

    /// Constructs and publishes this Warden's `HelloMessage`.
    async fn publish_hello_message(&self) -> anyhow::Result<()> {
        let (my_epoch, my_runid, master_name, master_addr) = {
            let state = self.state.lock();
            (
                state.config_epoch,
                self.global_state.my_run_id.clone(),
                state.config.name.clone(),
                state.addr,
            )
        };
        let hello_payload = HelloMessage {
            addr: self.my_announce_addr,
            run_id: my_runid,
            epoch: my_epoch,
            master_name,
            master_addr,
        };
        let message = serde_json::to_string(&hello_payload)?;
        self.publish_message(HELLO_CHANNEL.to_string(), message)
            .await
    }

    /// Processes a `HelloMessage` received from a peer Warden.
    fn process_hello_message(&self, payload: &Bytes) {
        let Ok(hello) = serde_json::from_slice::<HelloMessage>(payload) else {
            return;
        };

        if hello.run_id == self.global_state.my_run_id || hello.master_name != self.master_name {
            return;
        }

        let mut state = self.state.lock();
        let peer_entry =
            state
                .peers
                .entry(hello.run_id.clone())
                .or_insert_with(|| WardenPeerState {
                    run_id: hello.run_id,
                    addr: hello.addr,
                    last_hello_received: Instant::now(),
                });
        peer_entry.last_hello_received = Instant::now();
        debug!("Received hello from peer warden {}", peer_entry.run_id);
    }

    /// Checks if the master is subjectively down (SDOWN).
    async fn check_master_down(&self) {
        let (master_addr, down_after) = {
            let state = self.state.lock();
            (state.addr, state.config.down_after)
        };

        if self.ping_instance(master_addr).await.is_err() {
            let mut state = self.state.lock();
            if state.primary_state.down_since.is_none() {
                state.primary_state.down_since = Some(Instant::now());
            }

            if state
                .primary_state
                .down_since
                .expect("down_since is set")
                .elapsed()
                > down_after
                && state.status == MasterStatus::Ok
            {
                warn!(
                    "Master '{}' ({}) is subjectively down (SDOWN).",
                    state.config.name, state.addr
                );
                state.status = MasterStatus::Sdown;
            }
        } else {
            let mut state = self.state.lock();
            if state.primary_state.down_since.is_some() {
                info!(
                    "Master '{}' ({}) is back online.",
                    state.config.name, master_addr
                );
                state.primary_state.down_since = None;
                state.status = MasterStatus::Ok;
                state.reset_failover_state();
            }
        }
    }

    /// Checks the health of all known replicas.
    async fn check_replicas_down(&self) {
        let replicas_to_check: Vec<SocketAddr> = self
            .state
            .lock()
            .replicas
            .iter()
            .map(|e| *e.key())
            .collect();
        let down_after = self.state.lock().config.down_after * 2;

        for addr in replicas_to_check {
            if self.ping_instance(addr).await.is_ok() {
                if let Some(mut replica_state) = self.state.lock().replicas.get_mut(&addr) {
                    if replica_state.down_since.is_some() {
                        info!("Replica {} is back online.", addr);
                        replica_state.down_since = None;
                    }
                }
            } else if let Some(mut replica_state) = self.state.lock().replicas.get_mut(&addr) {
                if replica_state.down_since.is_none() {
                    replica_state.down_since = Some(Instant::now());
                } else if replica_state
                    .down_since
                    .expect("down_since is set")
                    .elapsed()
                    > down_after
                {
                    warn!(
                        "Replica {} for master '{}' is down.",
                        addr, self.master_name
                    );
                }
            }
        }
    }

    /// Periodically checks if any known replica is pointing to a stale master
    /// and proactively reconfigures it to follow the correct master.
    async fn reconfigure_stale_replicas(&self) {
        let (current_master_addr, current_master_runid, replicas_to_check) = {
            let state = self.state.lock();
            if state.failover_state != FailoverState::None {
                return;
            }
            (
                state.addr,
                state.run_id.clone(),
                state.replicas.iter().map(|e| *e.key()).collect::<Vec<_>>(),
            )
        };

        if current_master_runid == "?" {
            return;
        }

        for replica_addr in replicas_to_check {
            let should_spawn = {
                let mut state = self.state.lock();
                let lock_arc = state
                    .reconfigurations_in_progress
                    .entry(replica_addr)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone();
                // Attempt to acquire the lock. If it succeeds, we can spawn a task.
                // The lock guard is immediately dropped, but the Arc's strong count
                // signals that a task is "running".
                lock_arc.try_lock().is_some()
            };

            if should_spawn {
                let state_clone = self.state.clone();
                let master_addr_clone = current_master_addr;
                let master_runid_clone = current_master_runid.clone();

                tokio::spawn(async move {
                    reconfigure_and_verify_stale_replica_task(
                        replica_addr,
                        master_addr_clone,
                        master_runid_clone,
                    )
                    .await;

                    // After the task is done, remove the lock entry to allow future tasks.
                    let mut state = state_clone.lock();
                    state.reconfigurations_in_progress.remove(&replica_addr);
                });
            } else {
                debug!(
                    "Reconfiguration for replica {} is already in progress. Skipping.",
                    replica_addr
                );
            }
        }
    }

    /// Checks if a failover leader election should be started.
    async fn check_failover_status(&self) {
        let (should_check_election, master_down, quorum) = {
            let mut state = self.state.lock();
            let hello_timeout = state.config.hello_interval * 5;
            state
                .peers
                .retain(|_, peer| peer.last_hello_received.elapsed() < hello_timeout);
            (
                state.status == MasterStatus::Sdown && state.failover_state == FailoverState::None,
                state.primary_state.down_since.is_some(),
                state.config.quorum,
            )
        };

        if !should_check_election || !master_down {
            return;
        }

        let can_reach_quorum = {
            let state = self.state.lock();
            let total_wardens_seen = state.peers.len() + 1;

            if total_wardens_seen < quorum {
                warn!(
                    "Master '{}' is SDOWN, but this Warden can only see {}/{} required peers. Deferring failover election.",
                    self.master_name, total_wardens_seen, quorum
                );
                false
            } else {
                true
            }
        };

        if !can_reach_quorum {
            return;
        }

        let mut state = self.state.lock();
        if state.status != MasterStatus::Sdown || state.failover_state != FailoverState::None {
            return;
        }

        info!(
            "Master '{}' is subjectively down (SDOWN). Starting leader election.",
            state.config.name
        );
        state.failover_state = FailoverState::Vote;
        state.config_epoch += 1;
        state.last_voted_epoch = state.config_epoch;
        state.votes.clear();
        state
            .votes
            .insert(self.global_state.my_run_id.clone(), Instant::now());
        let vote_req_msg = format!(
            "VOTE-REQUEST:{}:{}:{}",
            self.master_name, self.global_state.my_run_id, state.config_epoch
        );
        drop(state);

        let self_clone = self.clone();
        tokio::spawn(async move {
            if let Err(e) = self_clone
                .publish_message(VOTES_CHANNEL.to_string(), vote_req_msg)
                .await
            {
                warn!("Failed to broadcast VOTE-REQUEST: {}", e);
            }
        });
    }

    /// Checks the results of an ongoing leader election.
    async fn check_election_status(&self) {
        let (in_vote_state, quorum, failover_timeout) = {
            let state = self.state.lock();
            (
                state.failover_state == FailoverState::Vote,
                state.config.quorum,
                state.config.failover_timeout,
            )
        };

        if !in_vote_state {
            return;
        }

        let mut state = self.state.lock();
        if state.votes.len() >= quorum {
            info!(
                "Won leader election for master '{}' with {} votes (quorum {}).",
                self.master_name,
                state.votes.len(),
                quorum
            );
            if state.last_failover_time.elapsed() < failover_timeout {
                warn!(
                    "Failover for '{}' already happened recently. Aborting and waiting for timeout.",
                    self.master_name
                );
                state.reset_failover_state();
                return;
            }

            state.failover_state = FailoverState::Start;
            state.failover_start_time = Some(Instant::now());
            info!(
                "Leader is starting failover process for master '{}'.",
                self.master_name
            );
            tokio::spawn(failover::start_failover(self.state.clone()));
        }
    }

    /// Processes a vote-related message from a peer Warden.
    async fn process_vote_message(&self, payload: &Bytes) {
        let payload_str = String::from_utf8_lossy(payload);
        let msg_parts: Vec<&str> = payload_str.split(':').collect();
        if msg_parts.len() < 4 {
            return;
        }

        let msg_type = msg_parts[0];
        let master_name = msg_parts[1];
        let candidate_id = msg_parts[2];
        let epoch: u64 = msg_parts[3].parse().unwrap_or(0);

        if master_name != self.master_name {
            return;
        }

        if msg_type == "VOTE-REQUEST" {
            let mut state = self.state.lock();
            if epoch > state.last_voted_epoch {
                info!(
                    "Voting for {} in epoch {} for master {}",
                    candidate_id, epoch, self.master_name
                );
                state.last_voted_epoch = epoch;

                let my_run_id = self.global_state.my_run_id.clone();
                let ack_msg = format!("VOTE-ACK:{master_name}:{my_run_id}:{candidate_id}:{epoch}");
                drop(state);

                let self_clone = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = self_clone
                        .publish_message(VOTES_CHANNEL.to_string(), ack_msg)
                        .await
                    {
                        warn!("Failed to broadcast VOTE-ACK: {}", e);
                    }
                });
            }
        } else if msg_type == "VOTE-ACK"
            && msg_parts.len() == 5
            && msg_parts[3] == self.global_state.my_run_id
        {
            let voter_id = candidate_id;
            let mut state = self.state.lock();
            if state.failover_state == FailoverState::Vote && epoch == state.config_epoch {
                info!(
                    "Received vote from {} for master {}",
                    voter_id, self.master_name
                );
                state.votes.insert(voter_id.to_string(), Instant::now());
            }
        }
    }

    /// Periodically polls the master for its `INFO REPLICATION` output.
    async fn poll_master_info(&self) {
        let master_addr = self.state.lock().addr;
        if let Ok(mut client) = WardenClient::connect(master_addr).await {
            if let Ok(info_str) = client.info_replication().await {
                let mut state = self.state.lock();
                self.parse_and_update_state(&mut state, &info_str);
            }
        }
    }

    /// Sends a PING to an instance to check its health.
    async fn ping_instance(&self, addr: SocketAddr) -> anyhow::Result<()> {
        let mut client = WardenClient::connect(addr).await?;
        client.ping().await?;
        Ok(())
    }

    /// Parses the `INFO REPLICATION` output and updates the master's state.
    fn parse_and_update_state(&self, state: &mut MasterState, info: &str) {
        let mut discovered_replicas = std::collections::HashSet::new();
        for line in info.lines() {
            if let Some(val) = line.strip_prefix("master_replid:") {
                state.run_id = val.trim().to_string();
            } else if line.starts_with("slave") {
                if let Some((_, val)) = line.split_once(':') {
                    let parts: HashMap<&str, &str> =
                        val.split(',').filter_map(|p| p.split_once('=')).collect();
                    if let (Some(ip), Some(port)) = (parts.get("ip"), parts.get("port")) {
                        if let Ok(addr) = format!("{ip}:{port}").parse::<SocketAddr>() {
                            discovered_replicas.insert(addr);
                            let offset: u64 = parts
                                .get("offset")
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0);
                            let mut entry = state
                                .replicas
                                .entry(addr)
                                .or_insert_with(|| InstanceState::new(addr));
                            entry.value_mut().replication_offset = offset;
                        }
                    }
                }
            }
        }
        state
            .replicas
            .retain(|addr, _| discovered_replicas.contains(addr));
    }
}

/// The actual logic of the reconfiguration task, extracted to be `Send`.
async fn reconfigure_and_verify_stale_replica_task(
    replica_addr: SocketAddr,
    current_master_addr: SocketAddr,
    current_master_runid: String,
) {
    if let Ok(mut client) = WardenClient::connect(replica_addr).await {
        if let Ok(info_str) = client.info_replication().await {
            let mut replica_role = "";
            let mut replica_master_runid = "";
            for line in info_str.lines() {
                if let Some(val) = line.strip_prefix("role:") {
                    replica_role = val.trim();
                }
                if let Some(val) = line.strip_prefix("master_replid:") {
                    replica_master_runid = val.trim();
                }
            }

            if replica_role == "slave" && replica_master_runid != current_master_runid {
                warn!(
                    "Detected stale replica {} pointing to master '{}' (should be '{}'). Reconfiguring.",
                    replica_addr, replica_master_runid, current_master_runid
                );

                let reconfigure_cmd = RespFrame::Array(vec![
                    RespFrame::BulkString("REPLICAOF".into()),
                    RespFrame::BulkString(current_master_addr.ip().to_string().into()),
                    RespFrame::BulkString(current_master_addr.port().to_string().into()),
                ]);

                if let Err(e) = client.send_and_receive(reconfigure_cmd).await {
                    warn!(
                        "Failed to send REPLICAOF to stale replica {}: {}",
                        replica_addr, e
                    );
                } else {
                    info!(
                        "Successfully sent REPLICAOF to stale replica {}",
                        replica_addr
                    );
                }
            }
        }
    }
}
