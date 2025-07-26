// src/core/state/replication.rs

//! Contains state definitions related to replication.

use crate::core::SpinelDBError;
use crate::core::state::ServerState;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// The synchronization state of a replica connected to this primary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplicaSyncState {
    /// The replica is waiting for a full resynchronization (SPLDB snapshot).
    AwaitingFullSync,
    /// The replica is online and receiving a live stream of commands.
    Online,
}

/// Runtime information about a connected replica.
#[derive(Debug, Clone)]
pub struct ReplicaStateInfo {
    /// The current synchronization state of the replica.
    pub sync_state: ReplicaSyncState,
    /// The last replication offset acknowledged by the replica.
    pub ack_offset: u64,
    /// The timestamp of the last acknowledgment received from the replica.
    pub last_ack_time: Instant,
}

/// Information about this server's role as a primary in replication.
#[derive(Debug)]
pub struct ReplicationInfo {
    /// The unique run ID of this primary.
    pub master_replid: String,
    /// The current global replication offset for this primary.
    pub master_repl_offset: AtomicU64,
}

/// Information about this server's role as a replica in replication.
#[derive(Debug, Default, Clone)]
pub struct ReplicaInfo {
    /// The run ID of the primary this replica is connected to.
    pub master_replid: String,
    /// The replication offset that this replica has processed.
    pub processed_offset: u64,
}

/// A serializable struct for persisting the poisoned masters map.
#[derive(Serialize, Deserialize)]
struct PoisonedMastersSerializable {
    /// Key: run_id, Value: expiry UNIX timestamp in seconds.
    entries: HashMap<String, u64>,
}

/// The central struct holding all replication-related state.
#[derive(Debug)]
pub struct ReplicationState {
    /// State relevant to when this server is a primary.
    pub replication_info: ReplicationInfo,
    /// State relevant to when this server is a replica. `None` if it's a primary.
    pub replica_info: tokio::sync::Mutex<Option<ReplicaInfo>>,
    /// A set of master run IDs that this replica should refuse to connect to.
    /// This is a safety mechanism used during Warden-led failovers to prevent
    /// connecting to a demoted (stale) primary.
    /// Key: Master run_id, Value: Expiry time for the poison entry.
    pub poisoned_masters: Arc<DashMap<String, Instant>>,
}

impl ReplicationState {
    /// The path to the file where poisoned masters are persisted.
    const POISONED_MASTERS_FILE: &'static str = "poisoned_masters.json";

    /// Creates a new `ReplicationState`.
    pub fn new(master_replid: String) -> Self {
        Self {
            replication_info: ReplicationInfo {
                master_replid,
                master_repl_offset: AtomicU64::new(0),
            },
            replica_info: tokio::sync::Mutex::new(None),
            poisoned_masters: Arc::new(DashMap::new()),
        }
    }

    /// Saves the current state of poisoned masters to a JSON file.
    pub fn save_poisoned_masters_to_disk(&self) -> Result<(), SpinelDBError> {
        info!("Saving poisoned masters state to disk.");
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Snapshot the DashMap, filter out expired entries, and convert Instants to UNIX timestamps.
        let entries: HashMap<String, u64> = self
            .poisoned_masters
            .iter()
            .filter_map(|entry| {
                let expiry_instant = *entry.value();
                if expiry_instant > Instant::now() {
                    let remaining_secs = expiry_instant.duration_since(Instant::now()).as_secs();
                    Some((entry.key().clone(), now_unix_secs + remaining_secs))
                } else {
                    None
                }
            })
            .collect();

        if entries.is_empty() {
            // If there are no entries, remove the file if it exists.
            if fs::metadata(Self::POISONED_MASTERS_FILE).is_ok() {
                fs::remove_file(Self::POISONED_MASTERS_FILE)?;
            }
            return Ok(());
        }

        let serializable = PoisonedMastersSerializable { entries };
        let json_data = serde_json::to_string(&serializable)?;

        // Atomically write the file by first writing to a temp file and then renaming.
        let temp_path = format!("{}.tmp", Self::POISONED_MASTERS_FILE);
        fs::write(&temp_path, json_data)?;
        fs::rename(&temp_path, Self::POISONED_MASTERS_FILE)?;
        Ok(())
    }

    /// Loads the poisoned masters state from a JSON file at startup.
    pub fn load_poisoned_masters_from_disk(&self) {
        info!("Loading poisoned masters state from disk.");
        match fs::read_to_string(Self::POISONED_MASTERS_FILE) {
            Ok(json_data) => {
                match serde_json::from_str::<PoisonedMastersSerializable>(&json_data) {
                    Ok(deserialized) => {
                        let now_unix_secs = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        for (run_id, expiry_timestamp) in deserialized.entries {
                            if expiry_timestamp > now_unix_secs {
                                let remaining_duration =
                                    Duration::from_secs(expiry_timestamp - now_unix_secs);
                                self.poisoned_masters
                                    .insert(run_id, Instant::now() + remaining_duration);
                            }
                        }
                        info!(
                            "Loaded {} valid poisoned master entries.",
                            self.poisoned_masters.len()
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to parse poisoned masters file, ignoring. Error: {}",
                            e
                        );
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // This is normal on first startup.
                info!("Poisoned masters file not found, starting fresh.");
            }
            Err(e) => {
                warn!(
                    "Failed to read poisoned masters file, ignoring. Error: {}",
                    e
                );
            }
        }
    }

    /// Atomically gets the current replication offset.
    pub fn get_replication_offset(&self) -> u64 {
        self.replication_info
            .master_repl_offset
            .load(Ordering::SeqCst)
    }

    /// Checks if a write command should be allowed based on the `min-replicas-to-write` policy.
    pub async fn check_min_replicas_policy(
        &self,
        server_state: &Arc<ServerState>,
    ) -> Result<(), SpinelDBError> {
        let config = server_state.config.lock().await;

        if let crate::config::ReplicationConfig::Primary(primary_config) = &config.replication {
            let min_replicas = primary_config.min_replicas_to_write;
            if min_replicas == 0 {
                return Ok(());
            }

            let max_lag = Duration::from_secs(primary_config.min_replicas_max_lag);
            let mut online_replicas = 0;

            for entry in server_state.replica_states.iter() {
                let info = entry.value();
                if info.sync_state == ReplicaSyncState::Online
                    && info.last_ack_time.elapsed() <= max_lag
                {
                    online_replicas += 1;
                }
            }

            if online_replicas < min_replicas {
                return Err(SpinelDBError::ReadOnly(format!(
                    "NOREPLICAS Not enough good replicas to write (have {online_replicas}, need {min_replicas})"
                )));
            }
        }
        Ok(())
    }
}
