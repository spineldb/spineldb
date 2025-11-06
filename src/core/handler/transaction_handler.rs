// src/core/handler/transaction_handler.rs

//! Manages the logic for SpinelDB-style transactions, including command queuing,
//! optimistic locking with `WATCH`, and atomic execution.

use crate::core::acl::user::AclUser;
use crate::core::commands::command_trait::{CommandExt, CommandFlags, WriteOutcome};
use crate::core::commands::generic::Eval as EvalCmd;
use crate::core::database::transaction::TransactionState;
use crate::core::database::{Db, ExecutionContext, ExecutionLocks, ShardCache};
use crate::core::events::{TransactionData, UnitOfWork};
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::{Command, RespValue, SpinelDBError};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::MutexGuard;
use tracing::error;

/// Handles the business logic for a transaction for a single client session.
pub struct TransactionHandler<'a> {
    state: Arc<ServerState>,
    db: &'a Arc<Db>,
    session_id: u64,
    authenticated_user: Option<Arc<AclUser>>,
}

impl<'a> TransactionHandler<'a> {
    /// Creates a new transaction handler for the given context.
    pub fn new(
        state: Arc<ServerState>,
        db: &'a Arc<Db>,
        session_id: u64,
        authenticated_user: Option<Arc<AclUser>>,
    ) -> Self {
        Self {
            state,
            db,
            session_id,
            authenticated_user,
        }
    }

    /// Handles the `MULTI` command, starting a new transaction for the session.
    pub fn handle_multi(&self) -> Result<RespValue, SpinelDBError> {
        if let Some(tx_state) = self.db.tx_states.get(&self.session_id)
            && tx_state.in_transaction
        {
            return Err(SpinelDBError::InvalidState(
                "MULTI calls can not be nested".to_string(),
            ));
        }
        self.db.start_transaction(self.session_id);
        Ok(RespValue::SimpleString("OK".into()))
    }

    /// Handles the queuing of a command after `MULTI` has been called.
    pub async fn handle_queueing(&self, command: Command) -> Result<RespValue, SpinelDBError> {
        let mut tx_state =
            self.db
                .tx_states
                .get_mut(&self.session_id)
                .ok_or(SpinelDBError::InvalidState(
                    "Command queued without MULTI".to_string(),
                ))?;

        if !tx_state.in_transaction {
            return Err(SpinelDBError::InvalidState(
                "Command queued without MULTI".to_string(),
            ));
        }

        if tx_state.has_error {
            return Ok(RespValue::Error(
                "EXECABORT Transaction discarded because of previous errors.".to_string(),
            ));
        }

        // Transform EVALSHA to EVAL at queue time to prevent race conditions with SCRIPT FLUSH.
        let command_to_queue = match command {
            Command::EvalSha(ref evalsha_cmd) => {
                if let Some(script_body) = self.state.scripting.get(&evalsha_cmd.sha1) {
                    Command::Eval(EvalCmd {
                        script: script_body,
                        num_keys: evalsha_cmd.num_keys,
                        keys: evalsha_cmd.keys.clone(),
                        args: evalsha_cmd.args.clone(),
                    })
                } else {
                    tx_state.has_error = true;
                    return Ok(RespValue::Error(
                        "NOSCRIPT No matching script. Please use EVAL.".to_string(),
                    ));
                }
            }
            _ if matches!(&command, Command::Watch(_))
                || command.get_flags().contains(CommandFlags::TRANSACTION)
                || command.get_flags().contains(CommandFlags::PUBSUB) =>
            {
                tx_state.has_error = true;
                return Ok(RespValue::Error(format!(
                    "ERR Command '{}' cannot be used in a transaction",
                    command.name()
                )));
            }
            _ => command.clone(),
        };

        tx_state.commands.push(command_to_queue);
        Ok(RespValue::SimpleString("QUEUED".into()))
    }

    /// Handles the `WATCH` command, registering keys for optimistic locking.
    pub async fn handle_watch(&self, keys: Vec<Bytes>) -> Result<RespValue, SpinelDBError> {
        if let Some(tx_state) = self.db.tx_states.get(&self.session_id)
            && tx_state.in_transaction
        {
            return Err(SpinelDBError::InvalidState(
                "WATCH inside MULTI is not allowed".to_string(),
            ));
        }
        self.db.watch_keys_in_tx(self.session_id, &keys).await?;
        Ok(RespValue::SimpleString("OK".into()))
    }

    /// Handles the `DISCARD` command, aborting the transaction.
    pub fn handle_discard(&self) -> Result<RespValue, SpinelDBError> {
        self.db.discard_transaction(self.session_id)?;
        Ok(RespValue::SimpleString("OK".into()))
    }

    /// Handles the `EXEC` command, attempting to execute the queued commands atomically.
    pub async fn handle_exec(&mut self) -> Result<RespValue, SpinelDBError> {
        let tx_state =
            self.db
                .take_transaction_state(self.session_id)
                .ok_or(SpinelDBError::InvalidState(
                    "EXEC without MULTI".to_string(),
                ))?;

        if tx_state.has_error {
            return Ok(RespValue::Error(
                "EXECABORT Transaction discarded because of previous errors.".to_string(),
            ));
        }

        if tx_state.commands.is_empty() && tx_state.watched_keys.is_empty() {
            return Ok(RespValue::Array(vec![]));
        }

        let (response, maybe_uow) = self.execute_transaction_atomically(tx_state).await?;

        if let Some(uow) = maybe_uow {
            self.state.event_bus.publish(uow, &self.state);
        }

        Ok(response)
    }

    /// The core logic for atomically executing a transaction.
    async fn execute_transaction_atomically(
        &mut self,
        tx_state: TransactionState,
    ) -> Result<(RespValue, Option<UnitOfWork>), SpinelDBError> {
        let all_keys = self.collect_all_keys(&tx_state);

        // Perform cluster cross-slot check before acquiring locks.
        if let Some(cluster_state) = &self.state.cluster
            && !all_keys.is_empty()
        {
            let first_slot = crate::core::cluster::slot::get_slot(&all_keys[0]);
            for key in all_keys.iter().skip(1) {
                if crate::core::cluster::slot::get_slot(key) != first_slot {
                    return Err(SpinelDBError::CrossSlot);
                }
            }
            if !cluster_state.i_own_slot(first_slot) {
                let owner_node = cluster_state.get_node_for_slot(first_slot);
                let addr =
                    owner_node.map_or_else(|| "".to_string(), |node| node.node_info.addr.clone());
                return Err(SpinelDBError::Moved {
                    slot: first_slot,
                    addr,
                });
            }
        }

        // The main critical section: acquire all locks and execute.
        {
            let mut guards = self.db.lock_shards_for_keys(&all_keys).await;

            // Check watched keys for modifications.
            if !self.check_watched_keys(&tx_state.watched_keys, &guards) {
                return Ok((RespValue::NullArray, None)); // Abort transaction.
            }

            let (responses, write_commands, total_keys_changed, has_flush) = self
                .execute_queued_commands(&tx_state.commands, &mut guards)
                .await;

            // Prepare the UnitOfWork for propagation if there were writes.
            let maybe_uow = if !write_commands.is_empty() || has_flush {
                if has_flush {
                    self.state
                        .persistence
                        .dirty_keys_counter
                        .store(0, Ordering::Relaxed);
                } else {
                    self.state
                        .persistence
                        .increment_dirty_keys(total_keys_changed);
                }

                Some(UnitOfWork::Transaction(Box::new(TransactionData {
                    all_commands: tx_state.commands,
                    write_commands,
                })))
            } else {
                None
            };

            Ok((RespValue::Array(responses), maybe_uow))
        } // All locks are released here.
    }

    /// Collects all unique keys from WATCH and the command queue.
    fn collect_all_keys(&self, tx_state: &TransactionState) -> Vec<Bytes> {
        let mut all_keys: Vec<Bytes> = tx_state.watched_keys.keys().cloned().collect();
        for command in &tx_state.commands {
            all_keys.extend(command.get_keys());
        }
        all_keys.sort_unstable();
        all_keys.dedup();
        all_keys
    }

    /// Checks if any watched keys have been modified since `WATCH` was called.
    fn check_watched_keys(
        &self,
        watched_keys: &HashMap<Bytes, Option<u64>>,
        guards: &BTreeMap<usize, MutexGuard<ShardCache>>,
    ) -> bool {
        if watched_keys.is_empty() {
            return true;
        }

        for (key, original_version_opt) in watched_keys {
            let shard_index = self.db.get_shard_index(key);
            if let Some(guard) = guards.get(&shard_index) {
                let current_version_opt = guard
                    .peek(key)
                    .filter(|e| !e.is_expired())
                    .map(|v| v.version);

                if current_version_opt != *original_version_opt {
                    return false; // Key was modified, abort.
                }
            } else {
                error!("Lock for watched key was not acquired during transaction.");
                return false;
            }
        }
        true
    }

    /// Executes the queued commands sequentially within the locked context.
    async fn execute_queued_commands<'b>(
        &self,
        commands: &[Command],
        guards: &'b mut BTreeMap<usize, MutexGuard<'a, ShardCache>>,
    ) -> (Vec<RespValue>, Vec<Command>, u64, bool) {
        let mut responses = Vec::with_capacity(commands.len());
        let mut successful_write_commands = Vec::new();
        let mut total_keys_changed = 0u64;
        let mut has_flush = false;
        let mut has_error = false;

        let mut temp_guards = std::mem::take(guards);

        for command in commands {
            if has_error {
                responses.push(RespValue::Error(
                    "EXECABORT Transaction discarded because of previous errors.".to_string(),
                ));
                continue;
            }

            // Re-check permissions right before execution to prevent race conditions.
            let resp_frame: RespFrame = command.clone().into();
            let raw_args = if let RespFrame::Array(mut arr) = resp_frame {
                arr.split_off(1)
            } else {
                vec![]
            };

            let keys_bytes = command.get_keys();
            let keys_as_strings: Vec<String> = keys_bytes
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();

            if !self.state.acl_enforcer.read().await.check_permission(
                self.authenticated_user.as_deref(),
                &raw_args,
                command.name(),
                command.get_flags(),
                &keys_as_strings,
                &[], // Pub/Sub channels are not relevant in a transaction.
            ) {
                has_error = true;
                responses.push(RespValue::Error(
                    "NOPERM An ACL rule preventing this command from executing was triggered."
                        .to_string(),
                ));
                continue;
            }

            let mut ctx = ExecutionContext {
                state: self.state.clone(),
                locks: ExecutionLocks::Multi {
                    guards: temp_guards,
                },
                db: self.db,
                command: Some(command.clone()),
                session_id: self.session_id,
                authenticated_user: self.authenticated_user.clone(),
            };

            let result = command.execute(&mut ctx).await;

            temp_guards = match ctx.locks {
                ExecutionLocks::Multi { guards } => guards,
                _ => unreachable!("Locks must be Multi during transaction execution"),
            };

            match result {
                Ok((resp, outcome)) => {
                    responses.push(resp);
                    if outcome != WriteOutcome::DidNotWrite
                        && !command.get_flags().contains(CommandFlags::NO_PROPAGATE)
                    {
                        successful_write_commands.push(command.clone());

                        match outcome {
                            WriteOutcome::Write { keys_modified } => {
                                total_keys_changed += keys_modified
                            }
                            WriteOutcome::Delete { keys_deleted } => {
                                total_keys_changed += keys_deleted
                            }
                            WriteOutcome::Flush => has_flush = true,
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    has_error = true;
                    responses.push(RespValue::Error(e.to_string()));
                }
            }
        }

        if has_error {
            successful_write_commands.clear();
            total_keys_changed = 0;
            has_flush = false;
        }

        *guards = temp_guards;

        (
            responses,
            successful_write_commands,
            total_keys_changed,
            has_flush,
        )
    }
}
