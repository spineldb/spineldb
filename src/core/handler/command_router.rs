// src/core/handler/command_router.rs

//! The central component for routing parsed commands to their appropriate handlers.
//!
//! The `Router` orchestrates the command processing pipeline. It receives a parsed `Command`
//! from the `ConnectionHandler` and subjects it to a series of checks and validations before
//! dispatching it for execution. This layered approach ensures correctness, security, and
//! adherence to the server's operational state (e.g., cluster mode, transactions, Pub/Sub).

use super::actions;
use super::pipeline::{acl_check, cluster_redirect, state_check};
use super::safety_guard;
use super::transaction_handler::TransactionHandler;
use crate::connection::SessionState;
use crate::core::commands::cache::command::CacheSubcommand;
use crate::core::commands::command_trait::{CommandExt, CommandFlags, WriteOutcome};
use crate::core::commands::generic::Eval as EvalCmd;
use crate::core::commands::generic::script::ScriptSubcommand;
use crate::core::commands::key_extractor;
use crate::core::database::{Db, ExecutionContext};
use crate::core::events::UnitOfWork;
use crate::core::metrics;
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::{Command, RespValue, SpinelDBError};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::fs::File as TokioFile;
use tracing::error;
use tracing::{Instrument, info_span};

/// Represents the various types of responses a command can produce.
pub enum RouteResponse {
    /// A single RESP value. This is the most common response type.
    Single(RespValue),
    /// Multiple RESP values, sent sequentially. Used for commands like `SUBSCRIBE`.
    Multiple(Vec<RespValue>),
    /// Streams a file body directly to the socket for high performance.
    StreamBody {
        resp_header: Vec<u8>,
        file: TokioFile,
    },
    /// No operation; no response should be sent to the client.
    NoOp,
}

/// The `Router` receives a parsed `Command` and directs it through the processing pipeline.
pub struct Router<'a> {
    state: Arc<ServerState>,
    session_id: u64,
    addr: SocketAddr,
    session: &'a mut SessionState,
}

impl<'a> Router<'a> {
    /// Creates a new `Router` for a given command and session.
    pub fn new(
        state: Arc<ServerState>,
        session_id: u64,
        addr: SocketAddr,
        session: &'a mut SessionState,
    ) -> Self {
        Self {
            state,
            session_id,
            addr,
            session,
        }
    }

    /// The main entry point for routing a command. It orchestrates the entire
    /// processing pipeline from validation to execution and metrics recording.
    pub async fn route(&mut self, command: Command) -> Result<RouteResponse, SpinelDBError> {
        // Emergency read-only check
        if command.get_flags().contains(CommandFlags::WRITE)
            && self.state.is_emergency_read_only.load(Ordering::Relaxed)
        {
            return Err(SpinelDBError::ReadOnly(
                "Server is in emergency read-only mode due to a critical propagation failure."
                    .into(),
            ));
        }

        let command_name = command.name();

        // Reconstruct the full RespFrame for ACL condition evaluation and latency monitoring.
        let resp_frame: RespFrame = command.clone().into();
        let (raw_cmd_name_frame, raw_args) = if let RespFrame::Array(mut arr) = resp_frame {
            (
                arr.first().cloned().unwrap_or(RespFrame::Null),
                arr.split_off(1),
            )
        } else {
            (RespFrame::Null, vec![])
        };
        let mut full_raw_args = vec![raw_cmd_name_frame];
        full_raw_args.extend_from_slice(&raw_args);

        let command_args_for_log = command.get_resp_args();

        // Instrument the entire command processing flow for observability.
        let span = info_span!(
            "command",
            name = %command_name,
            client.addr = %self.addr,
            client.id = %self.session_id,
        );

        async move {
            let start_time = Instant::now();
            self.state.stats.increment_total_commands();
            metrics::COMMANDS_PROCESSED_TOTAL.inc();

            // 1. Key Extraction: Determine which arguments are keys for cluster routing and ACLs.
            let keys_bytes = key_extractor::extract_keys_from_command(command_name, &raw_args)?;

            // Handle the one-shot ASKING command immediately.
            if let Command::Asking(_) = command {
                self.session.is_asking = true;
                return Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())));
            }

            // 2. Cluster Redirection Check: Return MOVED/ASK errors if the key is on another node.
            cluster_redirect::check_redirection(&self.state, &keys_bytes, self.session).await?;
            if self.session.is_asking {
                self.session.is_asking = false; // ASKING is a one-shot command.
            }

            // 3. ACL Check: Verify user permissions.
            acl_check::check_permissions(
                &self.state,
                self.session,
                &command,
                &full_raw_args,
                &keys_bytes,
            )
            .await?;

            // 4. Global State Check: Enforce read-only mode, min-replicas policy, etc.
            state_check::check_server_state(&self.state, &command).await?;

            // 5. Safety Guard Check: Prevent dangerous commands (e.g., KEYS on large DBs).
            safety_guard::check_safety_limits(&self.state, &command, self.session.current_db_index)
                .await?;

            // Dispatch command based on authentication and transaction state.
            let result = if !self.session.is_authenticated {
                self.handle_unauthenticated(command).await
            } else if self.session.is_in_transaction {
                self.handle_transaction_mode(command).await
            } else {
                self.handle_normal_command(command).await
            };

            // Record command latency for SLOWLOG and metrics.
            let latency = start_time.elapsed();
            metrics::COMMAND_LATENCY_SECONDS.observe(latency.as_secs_f64());
            self.state
                .latency_monitor
                .add_sample(command_name, command_args_for_log, latency);

            result
        }
        .instrument(span)
        .await
    }

    /// Handles commands when the session is not yet authenticated.
    async fn handle_unauthenticated(
        &mut self,
        command: Command,
    ) -> Result<RouteResponse, SpinelDBError> {
        if let Command::Auth(auth_cmd) = command {
            actions::auth::handle_auth(auth_cmd, self.session, &self.state).await
        } else {
            Err(SpinelDBError::AuthRequired)
        }
    }

    /// Handles commands when the session is inside a `MULTI`/`EXEC` block.
    async fn handle_transaction_mode(
        &mut self,
        command: Command,
    ) -> Result<RouteResponse, SpinelDBError> {
        // Transaction control commands are handled by the normal flow.
        if matches!(
            command,
            Command::Exec | Command::Discard | Command::Unwatch(_)
        ) {
            return self.handle_normal_command(command).await;
        }

        if matches!(command, Command::Watch(_)) {
            return Err(SpinelDBError::InvalidState(
                "WATCH inside MULTI is not allowed".to_string(),
            ));
        }

        let db = self.state.get_db(self.session.current_db_index).unwrap();
        TransactionHandler::new(
            self.state.clone(),
            &db,
            self.session_id,
            self.session.authenticated_user.clone(),
        )
        .handle_queueing(command)
        .await
        .map(RouteResponse::Single)
    }

    /// Handles the normal command flow by dispatching to specialized handlers or the generic executor.
    async fn handle_normal_command(
        &mut self,
        command: Command,
    ) -> Result<RouteResponse, SpinelDBError> {
        let db = self.state.get_db(self.session.current_db_index).unwrap();
        let state = self.state.clone();

        match command {
            // Special handling for commands that can return a streaming response.
            Command::Cache(ref cache_cmd) => {
                let mut ctx = self.build_exec_context(&command, &db).await;
                match &cache_cmd.subcommand {
                    CacheSubcommand::Get(get_cmd) => get_cmd.execute_and_stream(&mut ctx).await,
                    CacheSubcommand::Proxy(proxy_cmd) => {
                        proxy_cmd.execute_and_stream(&mut ctx).await
                    }
                    _ => self.execute_command(command, &db).await,
                }
            }

            // Connection state commands (modify session state directly).
            Command::Auth(cmd) => actions::auth::handle_auth(cmd, self.session, &state).await,
            Command::Quit(_) => Ok(RouteResponse::Single(RespValue::SimpleString("OK".into()))),
            Command::Select(cmd) => {
                actions::connection::handle_select(cmd, self.session, &state, self.session_id).await
            }

            // Transaction control commands.
            Command::Multi => {
                actions::transaction::handle_multi(&db, self.session, state, self.session_id)
            }
            Command::Exec => {
                actions::transaction::handle_exec(&db, self.session, state, self.session_id).await
            }
            Command::Discard => {
                actions::transaction::handle_discard(&db, self.session, state, self.session_id)
            }
            Command::Watch(cmd) => {
                actions::transaction::handle_watch(
                    cmd.keys,
                    &db,
                    state,
                    self.session_id,
                    self.session,
                )
                .await
            }
            Command::Unwatch(_) => actions::transaction::handle_unwatch(&db, self.session_id),

            // Pub/Sub commands (transition the connection into/out of Pub/Sub mode).
            Command::Subscribe(cmd) => actions::pubsub::handle_subscribe(
                cmd.channels,
                self.session,
                &state,
                &db,
                self.session_id,
            ),
            Command::PSubscribe(cmd) => actions::pubsub::handle_psubscribe(
                cmd.patterns,
                self.session,
                &state,
                &db,
                self.session_id,
            ),
            Command::Unsubscribe(cmd) => {
                actions::pubsub::handle_unsubscribe(cmd.channels, self.session)
            }
            Command::PUnsubscribe(cmd) => {
                actions::pubsub::handle_punsubscribe(cmd.patterns, self.session)
            }

            // Internal/Replication commands handled at the router level.
            Command::Replconf(ref cmd) => {
                actions::connection::handle_replconf(cmd, &state, &self.addr).await
            }
            Command::Psync(_) => Err(SpinelDBError::ReplicationError(
                "PSYNC should be handled by ConnectionHandler".into(),
            )),

            // All other standard commands are executed through the generic path.
            cmd => self.execute_command(cmd, &db).await,
        }
    }

    /// Builds an `ExecutionContext` for a given command, acquiring the necessary locks.
    async fn build_exec_context(&self, command: &Command, db: &'a Arc<Db>) -> ExecutionContext<'a> {
        ExecutionContext {
            state: self.state.clone(),
            locks: db.determine_locks_for_command(command).await,
            db,
            command: Some(command.clone()),
            session_id: self.session_id,
            authenticated_user: self.session.authenticated_user.clone(),
        }
    }

    /// Executes a standard command, handles eviction, propagates writes, and returns the response.
    async fn execute_command(
        &mut self,
        command: Command,
        db: &Arc<Db>,
    ) -> Result<RouteResponse, SpinelDBError> {
        // RAII guard to manage the in-flight counter for EVALSHA commands.
        // This ensures the counter is decremented even if the function exits early due to an error.
        struct EvalShaGuard(Arc<ServerState>);
        impl Drop for EvalShaGuard {
            fn drop(&mut self) {
                self.0.evalsha_in_flight.fetch_sub(1, Ordering::Relaxed);
            }
        }

        let _guard = if let Command::EvalSha(_) = command {
            self.state.evalsha_in_flight.fetch_add(1, Ordering::Relaxed);
            Some(EvalShaGuard(self.state.clone()))
        } else {
            None
        };

        // Special guard for SCRIPT FLUSH to prevent race conditions.
        if let Command::Script(ref script_cmd) = command
            && let ScriptSubcommand::Flush = script_cmd.subcommand
        {
            // Prevent FLUSH during an AOF rewrite.
            if self
                .state
                .persistence
                .aof_rewrite_state
                .lock()
                .await
                .is_in_progress
            {
                return Err(SpinelDBError::InvalidState(
                    "ERR SCRIPT FLUSH is not allowed while an AOF rewrite is in progress."
                        .to_string(),
                ));
            }

            // Prevent FLUSH while an EVALSHA is being processed to avoid a TOCTOU race condition.
            if self.state.evalsha_in_flight.load(Ordering::Relaxed) > 0 {
                return Err(SpinelDBError::InvalidState(
                    "ERR SCRIPT FLUSH is not allowed while an EVALSHA command is in progress."
                        .to_string(),
                ));
            }
        }

        // Proactively check for memory pressure before executing a write command.
        if command.get_flags().contains(CommandFlags::WRITE) {
            let (maxmemory, policy) = {
                let config = self.state.config.lock().await;
                (config.maxmemory, config.maxmemory_policy)
            };
            if let Some(maxmem) = maxmemory
                && policy != crate::config::EvictionPolicy::NoEviction
            {
                const MAX_EVICTION_ATTEMPTS: usize = 10;
                for _ in 0..MAX_EVICTION_ATTEMPTS {
                    let total_memory: usize = self
                        .state
                        .dbs
                        .iter()
                        .map(|db| db.get_current_memory())
                        .sum();
                    if total_memory < maxmem {
                        break;
                    }
                    if !db.evict_one_key(&self.state).await {
                        break;
                    }
                }
            }
        }

        // Build the execution context, which acquires the necessary locks.
        let mut ctx = self.build_exec_context(&command, db).await;
        let (resp_value, write_outcome) = command.execute(&mut ctx).await?;

        // If the command resulted in a write, handle propagation and statistics.
        if write_outcome != WriteOutcome::DidNotWrite {
            match write_outcome {
                WriteOutcome::Write { keys_modified } => {
                    self.state.persistence.increment_dirty_keys(keys_modified)
                }
                WriteOutcome::Delete { keys_deleted } => {
                    self.state.persistence.increment_dirty_keys(keys_deleted)
                }
                WriteOutcome::Flush => self
                    .state
                    .persistence
                    .dirty_keys_counter
                    .store(0, Ordering::Relaxed),
                WriteOutcome::DidNotWrite => {}
            }

            // Propagate the command to AOF/replicas unless flagged otherwise.
            if !command.get_flags().contains(CommandFlags::NO_PROPAGATE) {
                // Transform EVALSHA to EVAL for safe propagation. This ensures that even if a
                // SCRIPT FLUSH happens right after execution, the replication/AOF stream
                // receives the full script body, preventing desynchronization.
                let uow = if let Command::EvalSha(ref evalsha_cmd) = command {
                    if let Some(script_body) = self.state.scripting.get(&evalsha_cmd.sha1) {
                        UnitOfWork::Command(Box::new(Command::Eval(EvalCmd {
                            script: script_body,
                            num_keys: evalsha_cmd.num_keys,
                            keys: evalsha_cmd.keys.clone(),
                            args: evalsha_cmd.args.clone(),
                        })))
                    } else {
                        // This should be an unreachable state, because the command just executed successfully.
                        // If it happens, it's a critical logic error. We must prevent propagation.
                        error!(
                            "CRITICAL: Script for executed EVALSHA '{}' vanished before propagation. Command will NOT be propagated. Entering emergency read-only mode.",
                            evalsha_cmd.sha1
                        );
                        self.state
                            .is_emergency_read_only
                            .store(true, Ordering::Relaxed);
                        return Err(SpinelDBError::Internal(
                            "CRITICAL: Write operation could not be safely propagated. Server entering read-only mode.".into()
                        ));
                    }
                } else {
                    UnitOfWork::Command(Box::new(command))
                };
                self.state.event_bus.publish(uow, &self.state);
            }
        }
        Ok(RouteResponse::Single(resp_value))
    }
}
