// src/core/warden/failover.rs

//! Contains the core logic for performing an automated failover orchestrated by a Warden leader.

use super::client::WardenClient;
use super::state::{FailoverState, MasterState, MasterStatus};
use crate::core::protocol::RespFrame;
use parking_lot::Mutex;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{Duration, Instant, sleep};
use tracing::{debug, error, info, warn};

/// The main entry point for the failover process.
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
    let failover_timeout = {
        let mut state = state_arc.lock();
        state.status = MasterStatus::Ok;
        state.addr = candidate_addr;
        state.run_id = new_master_runid.clone();
        state.primary_state.down_since = None;
        state.last_failover_time = std::time::Instant::now();

        let other_replicas: HashSet<SocketAddr> = state
            .replicas
            .iter()
            .filter(|e| *e.key() != candidate_addr)
            .map(|e| *e.key())
            .collect();
        state.replicas_pending_reconfiguration = other_replicas;

        state.replicas.remove(&candidate_addr);
        state.failover_state = FailoverState::None;

        state.config.failover_timeout
    };

    // --- Step 6: Spawn a task to reconfigure all other replicas ---
    info!(
        "Spawning task to reconfigure remaining replicas and poison old master '{}'",
        old_master_runid
    );

    tokio::spawn(run_post_failover_reconfiguration(
        state_arc.clone(),
        candidate_addr,
        new_master_runid,
        old_master_runid,
        failover_timeout,
    ));

    info!(
        "Failover for master '{}' completed successfully. New master is {}.",
        master_name, candidate_addr
    );
}

/// Polls an instance until its `INFO REPLICATION` output shows `role:master`.
async fn wait_for_promotion(addr: SocketAddr) -> Option<String> {
    const PROMOTION_TIMEOUT_SECS: u64 = 15;
    const POLL_INTERVAL_SECS: u64 = 1;

    for _ in 0..(PROMOTION_TIMEOUT_SECS / POLL_INTERVAL_SECS) {
        if let Ok(mut client) = WardenClient::connect(addr).await
            && let Ok(info_str) = client.info_replication().await
        {
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

/// A background task to ensure all replicas are eventually reconfigured after a failover.
pub async fn run_post_failover_reconfiguration(
    state_arc: Arc<Mutex<MasterState>>,
    new_master_addr: SocketAddr,
    new_master_runid: String,
    old_master_runid: String,
    timeout: Duration,
) {
    let start_time = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        if state_arc.lock().replicas_pending_reconfiguration.is_empty() {
            break;
        }

        if start_time.elapsed() > timeout {
            warn!(
                "Post-failover reconfiguration timed out for master '{}'. Unreconfigured replicas: {:?}",
                state_arc.lock().config.name,
                state_arc.lock().replicas_pending_reconfiguration
            );
            break;
        }

        interval.tick().await;

        let replicas_to_process = state_arc.lock().replicas_pending_reconfiguration.clone();
        if replicas_to_process.is_empty() {
            continue;
        }

        for replica_addr in replicas_to_process {
            let res = reconfigure_and_verify_one_replica(
                replica_addr,
                new_master_addr,
                &new_master_runid,
                &old_master_runid,
            )
            .await;

            match res {
                Ok(true) => {
                    info!(
                        "Successfully verified reconfiguration for replica {}. Removing from processing queue.",
                        replica_addr
                    );
                    state_arc
                        .lock()
                        .replicas_pending_reconfiguration
                        .remove(&replica_addr);
                }
                Ok(false) => {
                    debug!(
                        "Replica {} has been commanded but not yet verified. Will retry.",
                        replica_addr
                    );
                }
                Err(e) => {
                    debug!(
                        "Failed to reconfigure replica {}: {}. Will retry.",
                        replica_addr, e
                    );
                }
            }
        }
    }

    state_arc.lock().reset_failover_state();
    info!(
        "Post-failover reconfiguration task finished for master '{}'.",
        state_arc.lock().config.name
    );
}

/// Helper to handle the command-and-verify logic for a single replica.
async fn reconfigure_and_verify_one_replica(
    replica_addr: SocketAddr,
    new_master_addr: SocketAddr,
    new_master_runid: &str,
    old_master_runid: &str,
) -> anyhow::Result<bool> {
    let mut client = WardenClient::connect(replica_addr).await?;

    // 1. Send the REPLICAOF command
    let replicaof_cmd = RespFrame::Array(vec![
        RespFrame::BulkString("REPLICAOF".into()),
        RespFrame::BulkString(new_master_addr.ip().to_string().into()),
        RespFrame::BulkString(new_master_addr.port().to_string().into()),
    ]);
    client.send_and_receive(replicaof_cmd).await?;

    // 2. Send the FAILOVER POISON command
    let poison_cmd = RespFrame::Array(vec![
        RespFrame::BulkString("FAILOVER".into()),
        RespFrame::BulkString("POISON".into()),
        RespFrame::BulkString(old_master_runid.to_string().into()),
        RespFrame::BulkString("60".into()), // Poison TTL of 60 seconds
    ]);
    client.send_and_receive(poison_cmd).await?;
    info!("Sent REPLICAOF and POISON commands to {}", replica_addr);

    // 3. Verify with INFO REPLICATION
    let info_str = client.info_replication().await?;
    for line in info_str.lines() {
        if let Some(val) = line.strip_prefix("master_replid:")
            && val.trim() == new_master_runid
        {
            return Ok(true); // Verification successful
        }
    }

    Ok(false) // Not yet verified
}
