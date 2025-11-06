// src/core/persistence/aof_loader.rs

//! Implements the logic for loading data from an Append-Only File (AOF)
//! into memory when the server starts.

use crate::core::commands::command_trait::CommandExt;
use crate::core::commands::generic::Select;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrameCodec;
use crate::core::state::ServerState;
use crate::core::{Command, SpinelDBError};
use bytes::{Bytes, BytesMut};
use std::mem;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::codec::Decoder;
use tracing::{debug, error, info, warn};

/// `AofLoader` is responsible for reading an AOF file and replaying the commands
/// to reconstruct the database state.
pub struct AofLoader {
    config: crate::config::PersistenceConfig,
}

impl AofLoader {
    pub fn new(config: crate::config::PersistenceConfig) -> Self {
        Self { config }
    }

    /// Loads the AOF file into the provided `ServerState`.
    ///
    /// It reads the AOF file in chunks, parses each RESP frame as a command,
    /// and executes it to rebuild the in-memory state. It correctly handles
    /// `SELECT` commands and `MULTI`/`EXEC` transaction blocks. This streaming
    /// approach avoids loading the entire file into memory at once.
    pub async fn load_into(&self, state: &Arc<ServerState>) -> Result<(), SpinelDBError> {
        if !self.config.aof_enabled {
            return Ok(());
        }

        let path = Path::new(&self.config.aof_path);
        if !path.exists() {
            info!(
                "AOF file not found at '{}', starting with an empty state.",
                self.config.aof_path
            );
            return Ok(());
        }

        info!("Loading data from AOF file: {}", self.config.aof_path);
        let file = TokioFile::open(path).await?;
        let mut reader = BufReader::new(file);
        let mut buffer = BytesMut::with_capacity(8192);

        let mut codec = RespFrameCodec;
        let mut commands_loaded = 0;
        let mut in_tx = false;
        let mut tx_commands: Vec<Command> = Vec::new();
        let mut current_db_index: usize = 0;

        // Read the file in chunks and decode frames iteratively.
        loop {
            if reader.read_buf(&mut buffer).await? == 0 {
                // End of file.
                if !buffer.is_empty() {
                    warn!("AOF file has trailing, incomplete data. Ignoring.");
                }
                break;
            }

            // Decode and process as many frames as possible from the current buffer.
            while let Some(frame) = codec.decode(&mut buffer)? {
                match Command::try_from(frame.clone()) {
                    Ok(command) => {
                        debug!("Loading from AOF, command: {:?}", command);
                        match command {
                            Command::Select(Select { db_index }) => {
                                if db_index < state.dbs.len() {
                                    current_db_index = db_index;
                                } else {
                                    warn!(
                                        "SELECT to out-of-range DB index {} in AOF file. Ignoring.",
                                        db_index
                                    );
                                }
                            }
                            Command::Multi => {
                                if in_tx {
                                    return Err(SpinelDBError::AofError(
                                        "Nested MULTI in AOF".into(),
                                    ));
                                }
                                in_tx = true;
                                tx_commands.clear();
                            }
                            Command::Exec => {
                                if !in_tx {
                                    return Err(SpinelDBError::AofError(
                                        "EXEC without MULTI in AOF".into(),
                                    ));
                                }
                                in_tx = false;

                                let commands_to_exec = mem::take(&mut tx_commands);

                                if !commands_to_exec.is_empty() {
                                    let db = state.get_db(current_db_index).ok_or_else(|| {
                                        SpinelDBError::AofError(format!(
                                            "Invalid DB index {current_db_index} during AOF transaction load"
                                        ))
                                    })?;

                                    let all_keys: Vec<Bytes> = commands_to_exec
                                        .iter()
                                        .flat_map(|c| c.get_keys())
                                        .collect();

                                    let mut guards = db.lock_shards_for_keys(&all_keys).await;

                                    for cmd in &commands_to_exec {
                                        let mut ctx = ExecutionContext {
                                            state: state.clone(),
                                            locks: ExecutionLocks::Multi {
                                                guards: mem::take(&mut guards),
                                            },
                                            db: &db,
                                            command: Some(cmd.clone()),
                                            session_id: 0,
                                            authenticated_user: None,
                                        };

                                        if let Err(e) = cmd.execute(&mut ctx).await {
                                            error!(
                                                "Error executing command {:?} from AOF transaction: {:?}. Aborting load.",
                                                cmd.name(),
                                                e
                                            );
                                            return Err(e);
                                        }

                                        guards = match ctx.locks {
                                            ExecutionLocks::Multi { guards } => guards,
                                            _ => unreachable!(
                                                "Locks must be Multi during transaction loading"
                                            ),
                                        };
                                    }
                                }
                                commands_loaded += 1;
                            }
                            Command::Discard => {
                                if !in_tx {
                                    return Err(SpinelDBError::AofError(
                                        "DISCARD without MULTI in AOF".into(),
                                    ));
                                }
                                in_tx = false;
                                tx_commands.clear();
                            }
                            cmd => {
                                if in_tx {
                                    tx_commands.push(cmd);
                                } else {
                                    self.execute_command(&cmd, state, current_db_index).await?;
                                    commands_loaded += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error parsing command from AOF file (file might be truncated): {}. Frame: {:?}. Continuing with loaded data.",
                            e, frame
                        );
                        break;
                    }
                }
            }
        }

        if in_tx {
            warn!("AOF file ends with an unclosed MULTI block. The transaction is discarded.");
        }

        info!(
            "Successfully loaded {} commands/transactions from AOF.",
            commands_loaded
        );
        Ok(())
    }

    /// Executes a single command against the database state during the loading process.
    async fn execute_command(
        &self,
        cmd: &Command,
        state: &Arc<ServerState>,
        db_index: usize,
    ) -> Result<(), SpinelDBError> {
        let db = state.get_db(db_index).ok_or_else(|| {
            SpinelDBError::Internal(format!("Invalid DB index {db_index} during AOF load"))
        })?;

        let locks = db.determine_locks_for_command(cmd).await;

        let mut ctx = ExecutionContext {
            state: state.clone(),
            locks,
            db: &db,
            command: Some(cmd.clone()),
            session_id: 0,
            authenticated_user: None,
        };

        if let Err(e) = cmd.execute(&mut ctx).await {
            error!(
                "Fatal error executing command `{:?}` from AOF file: {:?}. Aborting.",
                cmd, e
            );
            return Err(SpinelDBError::AofError(format!(
                "Failed to apply command from AOF: {e}"
            )));
        }
        Ok(())
    }
}
