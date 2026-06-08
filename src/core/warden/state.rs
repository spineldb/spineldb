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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_monitored_master() -> MonitoredMaster {
        MonitoredMaster {
            name: "m".to_string(),
            ip: "127.0.0.1".to_string(),
            port: 6379,
            quorum: 2,
            down_after: Duration::from_secs(15),
            failover_timeout: Duration::from_secs(60),
            hello_interval: Duration::from_secs(2),
        }
    }

    #[test]
    fn test_master_status_variants() {
        assert_ne!(MasterStatus::Ok, MasterStatus::Sdown);
        assert_ne!(MasterStatus::Sdown, MasterStatus::Odown);
        assert_ne!(MasterStatus::Ok, MasterStatus::Odown);
    }

    #[test]
    fn test_failover_state_variants() {
        assert_ne!(FailoverState::None, FailoverState::Wait);
        assert_ne!(FailoverState::Wait, FailoverState::Vote);
        assert_ne!(FailoverState::Vote, FailoverState::Start);
        assert_ne!(FailoverState::Start, FailoverState::SelectReplica);
        assert_ne!(FailoverState::SelectReplica, FailoverState::PromoteReplica);
    }

    #[test]
    fn test_instance_state_new_defaults() {
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let s = InstanceState::new(addr);
        assert_eq!(s.addr, addr);
        assert_eq!(s.run_id, "?");
        assert!(s.down_since.is_none());
        assert_eq!(s.replication_offset, 0);
    }

    #[test]
    fn test_master_state_from_starts_at_ok() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        assert_eq!(s.status, MasterStatus::Ok);
        assert_eq!(s.failover_state, FailoverState::None);
        assert_eq!(s.config_epoch, 0);
        assert_eq!(s.last_voted_epoch, 0);
        assert!(s.failover_start_time.is_none());
        assert!(s.promotion_candidate.is_none());
    }

    #[test]
    fn test_master_state_reset_failover_clears_all_failover_fields() {
        let cfg = make_monitored_master();
        let mut s = MasterState::from(cfg);
        // Simulate a failover in progress.
        s.failover_state = FailoverState::PromoteReplica;
        s.failover_start_time = Some(Instant::now());
        s.promotion_candidate = Some("127.0.0.1:6380".parse().unwrap());
        s.votes.insert("peer1".to_string(), Instant::now());
        s.replicas_pending_reconfiguration
            .insert("127.0.0.1:6381".parse().unwrap());

        s.reset_failover_state();

        assert_eq!(s.failover_state, FailoverState::None);
        assert!(s.failover_start_time.is_none());
        assert!(s.promotion_candidate.is_none());
        assert!(s.votes.is_empty());
        assert!(s.replicas_pending_reconfiguration.is_empty());
    }

    #[test]
    fn test_global_warden_state_starts_empty() {
        let g = GlobalWardenState {
            my_run_id: "test-run-id".to_string(),
            masters: DashMap::new(),
        };
        assert_eq!(g.my_run_id, "test-run-id");
        assert!(g.masters.is_empty());
    }

    #[test]
    fn test_global_warden_state_can_register_master() {
        let g = GlobalWardenState {
            my_run_id: "r".to_string(),
            masters: DashMap::new(),
        };
        let ms = Arc::new(Mutex::new(MasterState::from(make_monitored_master())));
        g.masters.insert("m1".to_string(), ms);
        assert_eq!(g.masters.len(), 1);
        assert!(g.masters.contains_key("m1"));
    }

    #[test]
    fn test_master_status_hash() {
        // Test that all variants are distinct by comparing them
        assert_ne!(MasterStatus::Ok, MasterStatus::Sdown);
        assert_ne!(MasterStatus::Ok, MasterStatus::Odown);
        assert_ne!(MasterStatus::Sdown, MasterStatus::Odown);
    }

    #[test]
    fn test_failover_state_all_variants_distinct() {
        let states = [
            FailoverState::None,
            FailoverState::Wait,
            FailoverState::Vote,
            FailoverState::Start,
            FailoverState::SelectReplica,
            FailoverState::PromoteReplica,
        ];
        for i in 0..states.len() {
            for j in (i + 1)..states.len() {
                assert_ne!(states[i], states[j]);
            }
        }
    }

    #[test]
    fn test_master_status_clone() {
        let s = MasterStatus::Odown;
        let c = s;
        assert_eq!(s, c);
    }

    #[test]
    fn test_failover_state_clone() {
        let s = FailoverState::PromoteReplica;
        let c = s;
        assert_eq!(s, c);
    }

    #[test]
    fn test_instance_state_clone() {
        let addr: SocketAddr = "10.0.0.1:6379".parse().unwrap();
        let mut s = InstanceState::new(addr);
        s.run_id = "abc123".to_string();
        s.replication_offset = 500;
        s.down_since = Some(Instant::now());
        let c = s.clone();
        assert_eq!(c.addr, addr);
        assert_eq!(c.run_id, "abc123");
        assert_eq!(c.replication_offset, 500);
        assert!(c.down_since.is_some());
    }

    #[test]
    fn test_master_state_addresses() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        assert_eq!(s.addr, "127.0.0.1:6379".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_master_state_config_preserved() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg.clone());
        assert_eq!(s.config.name, cfg.name);
        assert_eq!(s.config.ip, cfg.ip);
        assert_eq!(s.config.port, cfg.port);
        assert_eq!(s.config.quorum, cfg.quorum);
    }

    #[test]
    fn test_master_state_replicas_empty_initially() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        assert!(s.replicas.is_empty());
    }

    #[test]
    fn test_master_state_peers_empty_initially() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        assert!(s.peers.is_empty());
    }

    #[test]
    fn test_master_state_votes_empty_initially() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        assert!(s.votes.is_empty());
    }

    #[test]
    fn test_master_state_pending_reconfiguration_empty_initially() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        assert!(s.replicas_pending_reconfiguration.is_empty());
    }

    #[test]
    fn test_master_state_run_id_starts_unknown() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        assert_eq!(s.run_id, "?");
    }

    #[test]
    fn test_master_state_last_failover_time_is_old() {
        let cfg = make_monitored_master();
        let s = MasterState::from(cfg);
        // Should be at least 1 hour in the past
        assert!(s.last_failover_time.elapsed() > Duration::from_secs(3600));
    }

    #[test]
    fn test_global_warden_state_multiple_masters() {
        let g = GlobalWardenState {
            my_run_id: "r".to_string(),
            masters: DashMap::new(),
        };
        let cfg1 = MonitoredMaster {
            name: "m1".to_string(),
            ip: "127.0.0.1".to_string(),
            port: 6379,
            quorum: 2,
            down_after: Duration::from_secs(15),
            failover_timeout: Duration::from_secs(60),
            hello_interval: Duration::from_secs(2),
        };
        let cfg2 = MonitoredMaster {
            name: "m2".to_string(),
            ip: "127.0.0.1".to_string(),
            port: 6380,
            quorum: 1,
            down_after: Duration::from_secs(10),
            failover_timeout: Duration::from_secs(30),
            hello_interval: Duration::from_secs(1),
        };
        g.masters.insert(
            "m1".to_string(),
            Arc::new(Mutex::new(MasterState::from(cfg1))),
        );
        g.masters.insert(
            "m2".to_string(),
            Arc::new(Mutex::new(MasterState::from(cfg2))),
        );
        assert_eq!(g.masters.len(), 2);
    }
}
