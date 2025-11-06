// src/core/database/transaction.rs

use super::Db;
use crate::core::Command;
use crate::core::SpinelDBError;
use bytes::Bytes;
use std::collections::HashMap;
use tracing::debug;

/// Represents the state of an ongoing transaction for a specific session.
/// It holds the queue of commands to be executed and the keys being watched.
#[derive(Debug, Default, Clone)]
pub struct TransactionState {
    pub commands: Vec<Command>,
    pub watched_keys: HashMap<Bytes, Option<u64>>,
    /// Flag to indicate that `MULTI` has been called and the session is in a transaction.
    pub in_transaction: bool,
    /// Flag to mark the transaction as aborted due to an invalid command
    /// during the queuing phase (e.g., `SUBSCRIBE` inside `MULTI`).
    pub has_error: bool,
}

impl Db {
    /// Starts a new transaction for a given session ID.
    /// It marks the session as being in a transaction.
    pub fn start_transaction(&self, session_id: u64) {
        let mut tx_state = self.tx_states.entry(session_id).or_default();
        tx_state.commands.clear();
        tx_state.has_error = false;
        tx_state.in_transaction = true;
        debug!("Session {}: Started transaction.", session_id);
    }

    /// Queues a command within an ongoing transaction.
    /// The logic for validation is now primarily in `TransactionHandler`.
    ///
    /// # Errors
    ///
    /// Returns an error if the session is not currently in a transaction.
    pub fn queue_command_in_tx(
        &self,
        session_id: u64,
        command: Command,
    ) -> Result<(), SpinelDBError> {
        let mut tx_state =
            self.tx_states
                .get_mut(&session_id)
                .ok_or(SpinelDBError::InvalidState(
                    "Command queued without MULTI".to_string(),
                ))?;
        tx_state.commands.push(command);
        debug!("Session {}: Queued command.", session_id);
        Ok(())
    }

    /// Atomically retrieves the versions of all watched keys for optimistic locking.
    /// This is a critical operation for `WATCH`.
    pub async fn watch_keys_in_tx(
        &self,
        session_id: u64,
        keys: &[Bytes],
    ) -> Result<(), SpinelDBError> {
        if keys.is_empty() {
            debug!("Session {}: No keys to watch.", session_id);
            return Ok(());
        }

        // Get or create the transaction state for this session.
        // WATCH can be called before MULTI.
        let mut tx_state = self.tx_states.entry(session_id).or_default();

        let guards = self.lock_shards_for_keys(keys).await;

        for key in keys {
            let shard_index = self.get_shard_index(key);
            if let Some(guard) = guards.get(&shard_index) {
                // Get the version of the key if it exists and is not expired.
                let version_opt = guard
                    .peek(key)
                    .filter(|e| !e.is_expired())
                    .map(|v| v.version);
                tx_state.watched_keys.insert(key.clone(), version_opt);
                debug!(
                    "Session {}: Watched key {:?} with version {:?}.",
                    session_id, key, version_opt
                );
            }
        }
        Ok(())
    }

    /// Retrieves and removes the transaction state for a session.
    pub fn take_transaction_state(&self, session_id: u64) -> Option<TransactionState> {
        let state = self.tx_states.remove(&session_id).map(|(_, v)| v);
        if state.is_some() {
            debug!("Session {}: Took transaction state.", session_id);
        }
        state
    }

    /// Aborts a transaction, clearing all queued commands and watched keys for the session.
    pub fn discard_transaction(&self, session_id: u64) -> Result<(), SpinelDBError> {
        if self.tx_states.remove(&session_id).is_some() {
            debug!("Session {}: Discarded transaction.", session_id);
            Ok(())
        } else {
            // Per SpinelDB compatibility, DISCARD without MULTI is not an error.
            debug!(
                "Session {}: Discard called without active transaction.",
                session_id
            );
            Ok(())
        }
    }
}
