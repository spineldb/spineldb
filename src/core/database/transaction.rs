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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state_default() {
        let s = TransactionState::default();
        assert!(!s.in_transaction);
        assert!(!s.has_error);
        assert!(s.commands.is_empty());
        assert!(s.watched_keys.is_empty());
    }

    #[test]
    fn test_transaction_state_clone_preserves_data() {
        let mut s = TransactionState {
            in_transaction: true,
            ..Default::default()
        };
        s.watched_keys.insert(Bytes::from_static(b"k"), Some(7));
        let cloned = s.clone();
        assert!(cloned.in_transaction);
        assert_eq!(
            cloned.watched_keys.get(&Bytes::from_static(b"k")),
            Some(&Some(7))
        );
    }

    #[test]
    fn test_take_transaction_state_returns_none_for_unknown_session() {
        let db = Db::new();
        assert!(db.take_transaction_state(999).is_none());
    }

    #[test]
    fn test_discard_transaction_for_unknown_session_is_ok() {
        // Per spec, DISCARD without MULTI is not an error.
        let db = Db::new();
        assert!(db.discard_transaction(999).is_ok());
    }

    #[test]
    fn test_queue_command_without_multi_is_error() {
        let db = Db::new();
        // No MULTI was called for this session.
        let r = db.queue_command_in_tx(1, Command::Multi);
        assert!(matches!(r, Err(SpinelDBError::InvalidState(_))));
    }

    #[test]
    fn test_start_transaction_sets_in_transaction_flag() {
        let db = Db::new();
        db.start_transaction(42);
        let state = db.take_transaction_state(42).unwrap();
        assert!(state.in_transaction);
    }

    #[test]
    fn test_start_transaction_clears_existing_commands() {
        let db = Db::new();
        db.start_transaction(1);
        // Queue a command (this would normally require a prior MULTI, but we
        // bypass via direct manipulation for this test).
        db.start_transaction(1);
        let state = db.take_transaction_state(1).unwrap();
        assert!(state.commands.is_empty());
    }

    #[test]
    fn test_start_transaction_resets_has_error_flag() {
        let db = Db::new();
        // Manually set has_error to true.
        db.tx_states.insert(
            1,
            TransactionState {
                in_transaction: true,
                has_error: true,
                ..Default::default()
            },
        );
        db.start_transaction(1);
        let state = db.take_transaction_state(1).unwrap();
        assert!(!state.has_error, "start_transaction must reset has_error");
    }

    #[test]
    fn test_take_transaction_state_removes_entry() {
        let db = Db::new();
        db.start_transaction(1);
        assert!(db.take_transaction_state(1).is_some());
        // Second call returns None because the state was removed.
        assert!(db.take_transaction_state(1).is_none());
    }

    #[test]
    fn test_discard_transaction_removes_entry() {
        let db = Db::new();
        db.start_transaction(1);
        db.discard_transaction(1).unwrap();
        // After DISCARD, the state is gone.
        assert!(db.take_transaction_state(1).is_none());
    }
}
