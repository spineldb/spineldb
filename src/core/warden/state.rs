// src/core/warden/state.rs

//! Defines all data structures used by Ignis Warden to maintain the state
//! of the monitored SpinelDB instances and the failover process.

use super::client::WardenClient;
use super::config::MonitoredMaster;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Represents the perceived status of a master instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MasterStatus {
    /// The master is healthy and responding.
    Ok,
    /// Subjective Down: This Warden instance believes the master is down.
    Sdown,
    /// Objective Down: A quorum of Wardens agrees that the master is down.
    Odown,
}

/// Represents the state of a single database instance (primary, replica, or another Warden).
#[derive(Debug, Clone)]
pub struct InstanceState {
    /// The network address (IP:port) of the instance.
    pub addr: SocketAddr,
    /// The unique 40-character run ID of the instance.
    pub run_id: String,
    /// The last time a successful PONG was received from this instance.
    pub last_pong_received: Instant,
    /// The time when this instance was first detected as being down. `None` if it's up.
    pub down_since: Option<Instant>,
    /// The last known replication offset (for replicas).
    pub replication_offset: u64,
}

impl InstanceState {
    /// Creates a new `InstanceState` with default values for a newly discovered instance.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            run_id: "?".to_string(), // Initially unknown
            last_pong_received: Instant::now(),
            down_since: None,
            replication_offset: 0,
        }
    }
}

/// State for a discovered peer Warden instance monitoring the same master.
#[derive(Debug, Clone)]
pub struct WardenPeerState {
    pub run_id: String,
    pub addr: SocketAddr,
    pub last_hello_received: Instant,
}

/// Represents the different stages of an automated failover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverState {
    /// No failover is in progress.
    None,
    /// Waiting for a timeout before starting the failover (e.g., failover-timeout).
    Wait,
    /// The Warden is in the process of leader election by gathering votes.
    Vote,
    /// The failover process has been triggered and is starting.
    Start,
    /// A replica has been chosen for promotion.
    SelectReplica,
    /// The chosen replica is being promoted to a master.
    PromoteReplica,
}

/// Holds all the dynamic state for a single monitored master and its replicas.
/// This struct is protected by a Mutex to ensure thread-safe updates.
#[derive(Debug)]
pub struct MasterState {
    /// The static configuration for this master from `warden.toml`.
    pub config: MonitoredMaster,
    /// The current status of the master (Ok, Sdown, Odown).
    pub status: MasterStatus,
    /// The current network address of the primary. This changes upon failover.
    pub addr: SocketAddr,
    /// The unique run ID of the current primary.
    pub run_id: String,
    /// The dynamic state of the primary instance itself.
    pub primary_state: InstanceState,
    /// A map of all known replicas for this master, keyed by their address.
    pub replicas: DashMap<SocketAddr, InstanceState>,
    /// A map of other Warden instances monitoring this same master.
    /// Key: Warden's run_id.
    pub peers: HashMap<String, WardenPeerState>,
    /// The client used for Pub/Sub communication with the master.
    /// Wrapped in a Mutex to allow reconnection if the connection drops.
    pub pubsub_client: Mutex<Option<WardenClient>>,
    /// The configuration epoch, incremented on each successful failover.
    pub config_epoch: u64,
    /// The current state of the failover process for this master.
    pub failover_state: FailoverState,
    /// The time when the current failover process began.
    pub failover_start_time: Option<Instant>,
    /// The address of the replica selected to be the new master.
    pub promotion_candidate: Option<SocketAddr>,
    /// A record of votes received from other Wardens during leader election for a failover.
    /// Key: Warden's run_id, Value: Timestamp of the vote.
    pub votes: HashMap<String, Instant>,
    /// The last time a failover was successfully completed for this master.
    pub last_failover_time: Instant,
    /// The last epoch this Warden has cast a vote for, preventing duplicate voting.
    pub last_voted_epoch: u64,
    /// [BARU] A set of replica addresses that still need to be reconfigured after a failover.
    /// This state is persisted across Warden restarts (in memory).
    pub replicas_pending_reconfiguration: HashSet<SocketAddr>,
}

impl MasterState {
    /// Creates a new `MasterState` from its static configuration.
    pub fn from(config: MonitoredMaster) -> Self {
        let addr: SocketAddr = format!("{}:{}", config.ip, config.port)
            .parse()
            .expect("Invalid master address in config");
        Self {
            config,
            status: MasterStatus::Ok,
            addr,
            run_id: "?".to_string(),
            primary_state: InstanceState::new(addr),
            replicas: DashMap::new(),
            peers: HashMap::new(),
            pubsub_client: Mutex::new(None),
            config_epoch: 0,
            failover_state: FailoverState::None,
            failover_start_time: None,
            promotion_candidate: None,
            votes: HashMap::new(),
            // Initialize with a time far in the past to allow the first failover immediately.
            last_failover_time: Instant::now() - Duration::from_secs(3600 * 24),
            last_voted_epoch: 0,
            // [BARU] Initialize the new set.
            replicas_pending_reconfiguration: HashSet::new(),
        }
    }

    /// Resets the failover-related fields to their default state.
    pub fn reset_failover_state(&mut self) {
        self.failover_state = FailoverState::None;
        self.failover_start_time = None;
        self.promotion_candidate = None;
        self.votes.clear();
        // [BARU] Also clear any pending reconfiguration tasks.
        self.replicas_pending_reconfiguration.clear();
    }
}

/// The top-level, globally shared state for the entire Warden process.
#[derive(Debug)]
pub struct GlobalWardenState {
    /// The unique run ID of this Warden instance.
    pub my_run_id: String,
    /// A thread-safe map from a master's name to its `MasterState`.
    pub masters: DashMap<String, Arc<Mutex<MasterState>>>,
}
