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
use crate::core::commands::key_extractor;
use crate::core::events::UnitOfWork;
use crate::core::metrics;
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::storage::db::{Db, ExecutionContext};
use crate::core::{Command, RespValue, SpinelDBError};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::fs::File as TokioFile;
use tracing::error;
use tracing::{Instrument, info_span};

/// Represents the various types of responses a command can produce.
/// This enum allows the `ConnectionHandler` to handle different response strategies,
/// such as simple replies, multiple replies, or efficient file streaming.
pub enum RouteResponse {
    /// A single RESP value. This is the most common response type.
    Single(RespValue),
    /// Multiple RESP values, sent sequentially. Used for commands like `SUBSCRIBE`.
    Multiple(Vec<RespValue>),
    /// Streams a file body directly to the socket for high performance.
    /// The `resp_header` is the RESP Bulk String header (e.g., "$12345\r\n").
    /// This is a key optimization for the Intelligent Caching feature.
    StreamBody {
        resp_header: Vec<u8>,
        file: TokioFile,
    },
    /// No operation; no response should be sent to the client. This is used by
    /// commands that do not have a reply, like a successful `CACHE.GET` with `if-none-match`.
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
        let command_name = command.name();

        // Reconstruct the full RespFrame. This is necessary for two reasons:
        // 1. To provide the raw arguments to the ACL enforcer for condition evaluation.
        // 2. To pass the full command to the latency monitor for `SLOWLOG`.
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

        // Instrument the entire command processing flow for better observability.
        // The span includes key metadata about the command and client.
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

            // --- COMMAND PROCESSING PIPELINE ---
            // Each of the following functions acts as a gatekeeper. If a check fails,
            // it returns an `Err`, immediately halting the command's execution and
            // sending an appropriate error to the client.

            // 1. Key Extraction: Determine which arguments are keys. This is a prerequisite
            //    for cluster redirection and ACL checks.
            let keys_bytes = key_extractor::extract_keys_from_command(command_name, &raw_args)?;

            // Handle the one-shot ASKING command immediately, as it modifies session state
            // for the *next* command.
            if let Command::Asking(_) = command {
                self.session.is_asking = true;
                return Ok(RouteResponse::Single(RespValue::SimpleString("OK".into())));
            }

            // 2. Cluster Redirection Check: For cluster mode, determines if the command
            //    targets a slot owned by another node and returns a MOVED or ASK error if so.
            cluster_redirect::check_redirection(&self.state, &keys_bytes, self.session).await?;
            if self.session.is_asking {
                self.session.is_asking = false; // ASKING is a one-shot command.
            }

            // 3. ACL Check: Verifies if the authenticated user has permission to execute
            //    this command on the specified keys and channels.
            acl_check::check_permissions(
                &self.state,
                self.session,
                &command,
                &full_raw_args,
                &keys_bytes,
            )
            .await?;

            // 4. Global State Check: Ensures the command is allowed in the current server
            //    state (e.g., blocks writes if in read-only mode or if the min-replicas
            //    policy is not met).
            state_check::check_server_state(&self.state, &command).await?;

            // 5. Safety Guard Check: Prevents potentially dangerous commands that could
            //    block the server, like `KEYS` on a huge database, if configured.
            safety_guard::check_safety_limits(&self.state, &command, self.session.current_db_index)
                .await?;
            // --- END OF PIPELINE ---

            // If all checks pass, dispatch the command for execution based on session state.
            let result = if !self.session.is_authenticated {
                self.handle_unauthenticated(command).await
            } else if self.session.is_in_transaction {
                self.handle_transaction_mode(command).await
            } else {
                self.handle_normal_command(command).await
            };

            // Record command latency for SLOWLOG and Prometheus metrics.
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

    /// Handles commands when the session is not yet authenticated. Only `AUTH` is allowed.
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
    /// Most commands are queued, while `EXEC`, `DISCARD`, and `UNWATCH` are handled immediately.
    async fn handle_transaction_mode(
        &mut self,
        command: Command,
    ) -> Result<RouteResponse, SpinelDBError> {
        // These commands control the transaction itself and are handled by the normal flow.
        if matches!(
            command,
            Command::Exec | Command::Discard | Command::Unwatch(_)
        ) {
            return self.handle_normal_command(command).await;
        }

        // WATCH is not allowed inside a MULTI block.
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

    /// Handles the normal command flow by dispatching to specialized action handlers
    /// or the generic command executor.
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
                        // `CACHE.PROXY` orchestrates `GET` and `FETCH` and may result in
                        // a streaming response from its internal `GET` call.
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
        // Proactively check for memory pressure before executing a write command.
        if command.get_flags().contains(CommandFlags::WRITE) {
            let (maxmemory, policy) = {
                let config = self.state.config.lock().await;
                (config.maxmemory, config.maxmemory_policy)
            };
            if let Some(maxmem) = maxmemory {
                if policy != crate::config::EvictionPolicy::NoEviction {
                    const MAX_EVICTION_ATTEMPTS: usize = 10;
                    for _ in 0..MAX_EVICTION_ATTEMPTS {
                        let total_memory: usize = self
                            .state
                            .dbs
                            .iter()
                            .map(|db| db.get_current_memory())
                            .sum();
                        if total_memory < maxmem {
                            break; // Enough memory, proceed.
                        }
                        // Attempt to evict a key to make space.
                        if !db.evict_one_key(&self.state).await {
                            break; // No keys could be evicted.
                        }
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
                // Transform EVALSHA to EVAL for safe propagation, ensuring replicas
                // and AOF files are self-contained.
                let uow = if let Command::EvalSha(evalsha_cmd) = &command {
                    if let Some(script_body) = self.state.scripting.get(&evalsha_cmd.sha1) {
                        UnitOfWork::Command(Box::new(Command::Eval(EvalCmd {
                            script: script_body,
                            num_keys: evalsha_cmd.num_keys,
                            keys: evalsha_cmd.keys.clone(),
                            args: evalsha_cmd.args.clone(),
                        })))
                    } else {
                        // This indicates a critical state inconsistency.
                        error!(
                            "CRITICAL: Script for executed EVALSHA '{}' vanished before propagation.",
                            evalsha_cmd.sha1
                        );
                        UnitOfWork::Command(Box::new(command))
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
