// src/core/warden/failover.rs

//! Contains the core logic for performing an automated failover orchestrated by a Warden leader.
//!
//! This module is invoked by a `MasterMonitor` task after it has won a leader election.
//! It is responsible for selecting the best replica, promoting it to a new primary,
//! and reconfiguring all other replicas (and the old primary, if reachable) to follow the
//! new primary. This process is designed to be as safe and atomic as possible.

use super::client::WardenClient;
use super::state::{FailoverState, MasterState, MasterStatus};
use crate::core::protocol::RespFrame;
use parking_lot::Mutex;
use std::cmp::Ordering;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

/// The main entry point for the failover process. This function orchestrates all steps.
/// It is spawned as a new task by the `MasterMonitor` that won the leader election.
pub async fn start_failover(state_arc: Arc<Mutex<MasterState>>) {
    let (master_name, old_master_addr, old_master_runid) = {
        let state = state_arc.lock();
        (state.config.name.clone(), state.addr, state.run_id.clone())
    };
    info!("Starting failover process for master '{}'", master_name);

    // --- Step 1: Select the best replica to promote ---
    let candidate_addr = {
        let mut state = state_arc.lock();
        if state.failover_state != FailoverState::Start {
            warn!("start_failover called but state is not 'Start'. Aborting.");
            return;
        }

        let candidate = select_best_replica(&state);
        if candidate.is_none() {
            warn!(
                "No suitable replica found to promote for master '{}'. Aborting failover.",
                state.config.name
            );
            state.reset_failover_state();
            return;
        }

        let candidate_addr = candidate.unwrap();
        state.failover_state = FailoverState::PromoteReplica;
        state.promotion_candidate = Some(candidate_addr);
        candidate_addr
    };

    info!(
        "Selected replica {} as promotion candidate for master '{}'.",
        candidate_addr, master_name
    );

    // Perform a final health check on the candidate right before attempting promotion.
    if WardenClient::connect(candidate_addr).await.is_err() {
        warn!(
            "Promotion candidate {} failed final health check. Aborting failover.",
            candidate_addr
        );
        state_arc.lock().reset_failover_state();
        return;
    }

    // --- Step 2: Send `REPLICAOF NO ONE` to promote the candidate ---
    info!("Sending REPLICAOF NO ONE to {}", candidate_addr);
    let promotion_success = match WardenClient::connect(candidate_addr).await {
        Ok(mut client) => {
            let cmd = RespFrame::Array(vec![
                RespFrame::BulkString("REPLICAOF".into()),
                RespFrame::BulkString("NO".into()),
                RespFrame::BulkString("ONE".into()),
            ]);
            client.send_and_receive(cmd).await.is_ok()
        }
        Err(e) => {
            error!(
                "Failed to connect to promotion candidate {}: {}",
                candidate_addr, e
            );
            false
        }
    };

    if !promotion_success {
        warn!("Failed to send REPLICAOF NO ONE to candidate. Aborting failover.");
        state_arc.lock().reset_failover_state();
        return;
    }

    // --- Step 3: Wait and verify that the promotion was successful ---
    let new_master_runid = match wait_for_promotion(candidate_addr).await {
        Some(runid) => runid,
        None => {
            warn!(
                "Candidate {} did not transition to master role in time. Aborting failover.",
                candidate_addr
            );
            state_arc.lock().reset_failover_state();
            return;
        }
    };
    info!(
        "Successfully promoted {} to be the new master for '{}' (new runid: {})",
        candidate_addr, master_name, new_master_runid
    );

    // --- Step 4: Attempt to demote the old master to a replica of the new master ---
    info!("Attempting to demote old master at {}", old_master_addr);
    let new_master_ip_str = candidate_addr.ip().to_string();
    let new_master_port_str = candidate_addr.port().to_string();

    match WardenClient::connect(old_master_addr).await {
        Ok(mut client) => {
            let cmd = RespFrame::Array(vec![
                RespFrame::BulkString("REPLICAOF".into()),
                RespFrame::BulkString(new_master_ip_str.clone().into()),
                RespFrame::BulkString(new_master_port_str.clone().into()),
            ]);
            if let Err(e) = client.send_and_receive(cmd).await {
                warn!(
                    "Failed to send REPLICAOF to old master {}: {}. It may be unreachable.",
                    old_master_addr, e
                );
            } else {
                info!(
                    "Successfully sent demotion command to old master {}",
                    old_master_addr
                );
            }
        }
        Err(e) => {
            warn!(
                "Could not connect to old master {} to demote it: {}",
                old_master_addr, e
            );
        }
    }

    // --- Step 5: Update the Warden's internal state with the new primary information ---
    let other_replicas = {
        let mut state = state_arc.lock();
        state.status = MasterStatus::Ok;
        state.addr = candidate_addr;
        state.run_id = new_master_runid;
        state.primary_state.down_since = None;
        state.last_failover_time = std::time::Instant::now();

        let replicas: Vec<SocketAddr> = state
            .replicas
            .iter()
            .filter(|e| *e.key() != candidate_addr)
            .map(|e| *e.key())
            .collect();

        state.replicas.remove(&candidate_addr);
        state.reset_failover_state();
        replicas
    };

    // --- Step 6: Reconfigure all other replicas and poison the old master's run ID ---
    info!(
        "Reconfiguring {} other replica(s) to follow new master at {}.",
        other_replicas.len(),
        candidate_addr
    );

    let new_master_ip = candidate_addr.ip().to_string();
    let new_master_port = candidate_addr.port();

    for replica_addr in other_replicas {
        info!(
            "Spawning task to reconfigure replica {} and poison old master '{}'",
            replica_addr, old_master_runid
        );

        let ip_clone = new_master_ip.clone();
        let old_runid_clone = old_master_runid.clone();

        tokio::spawn(async move {
            match WardenClient::connect(replica_addr).await {
                Ok(mut client) => {
                    let replicaof_cmd = RespFrame::Array(vec![
                        RespFrame::BulkString("REPLICAOF".into()),
                        RespFrame::BulkString(ip_clone.into()),
                        RespFrame::BulkString(new_master_port.to_string().into()),
                    ]);
                    if let Err(e) = client.send_and_receive(replicaof_cmd).await {
                        warn!(
                            "Failed to send REPLICAOF to replica {}: {}",
                            replica_addr, e
                        );
                    } else {
                        info!("Successfully sent REPLICAOF to replica {}", replica_addr);
                    }

                    let poison_cmd = RespFrame::Array(vec![
                        RespFrame::BulkString("FAILOVER".into()),
                        RespFrame::BulkString("POISON".into()),
                        RespFrame::BulkString(old_runid_clone.into()),
                        RespFrame::BulkString("60".into()),
                    ]);
                    if let Err(e) = client.send_and_receive(poison_cmd).await {
                        warn!(
                            "Failed to send FAILOVER POISON to replica {}: {}",
                            replica_addr, e
                        );
                    } else {
                        info!(
                            "Successfully poisoned old master on replica {}",
                            replica_addr
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to replica {} for reconfiguration: {}",
                        replica_addr, e
                    );
                }
            }
        });
    }

    info!(
        "Failover for master '{}' completed successfully. New master is {}.",
        master_name, candidate_addr
    );
}

/// Polls an instance until its `INFO REPLICATION` output shows `role:master`.
/// Returns the new `master_replid` on success, or `None` on timeout.
async fn wait_for_promotion(addr: SocketAddr) -> Option<String> {
    const PROMOTION_TIMEOUT_SECS: u64 = 15;
    const POLL_INTERVAL_SECS: u64 = 1;

    for _ in 0..(PROMOTION_TIMEOUT_SECS / POLL_INTERVAL_SECS) {
        if let Ok(mut client) = WardenClient::connect(addr).await {
            if let Ok(info_str) = client.info_replication().await {
                let mut role = None;
                let mut runid = None;
                for line in info_str.lines() {
                    if let Some(val) = line.strip_prefix("role:") {
                        role = Some(val.trim());
                    }
                    if let Some(val) = line.strip_prefix("master_replid:") {
                        runid = Some(val.trim().to_string());
                    }
                }
                if role == Some("master") {
                    return runid;
                }
            }
        }
        sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
    }
    None
}

/// Selects the best replica for promotion based on standard Sentinel criteria.
fn select_best_replica(state: &MasterState) -> Option<SocketAddr> {
    state
        .replicas
        .iter()
        .filter(|entry| entry.value().down_since.is_none())
        .max_by(|a, b| {
            let a_val = a.value();
            let b_val = b.value();
            match a_val.replication_offset.cmp(&b_val.replication_offset) {
                Ordering::Equal => b_val.run_id.cmp(&a_val.run_id),
                other => other,
            }
        })
        .map(|entry| *entry.key())
}
