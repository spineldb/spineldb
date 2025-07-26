// src/core/persistence/aof_rewriter.rs

//! Implements the AOF (Append-Only File) rewrite logic.
//!
//! The rewrite process creates a new, compact AOF file containing the minimal set
//! of commands required to reconstruct the current database state. This is done
//! in a background task to avoid blocking the main server loop, which is a critical
//! design pattern for maintaining high performance and responsiveness. The process is
//! carefully orchestrated to handle concurrent writes safely.

use crate::core::commands::generic::script::ScriptSubcommand;
use crate::core::commands::generic::{Eval as EvalCmd, Script as ScriptCmd};
use crate::core::events::{PropagatedWork, UnitOfWork};
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::storage::data_types::StoredValue;
use crate::core::{Command, SpinelDBError};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// The main entry point for the AOF rewrite process.
///
/// This function orchestrates the rewrite by:
/// 1. Setting a flag to begin buffering incoming write commands in memory.
/// 2. Spawning the I/O-intensive database snapshotting work onto a dedicated blocking thread.
/// 3. Waiting for the blocking task to complete.
/// 4. Appending the buffered commands to the newly created temporary AOF file.
/// 5. Signaling the main AOF writer task to atomically switch to the new file.
///
/// This multi-stage process ensures that the server remains responsive during the rewrite
/// and that no write operations are lost.
pub async fn rewrite_aof(state: Arc<ServerState>) {
    info!("AOF rewrite process started by worker task.");

    let scripts_snapshot;
    {
        // Atomically acquire a lock and set the rewrite_in_progress flag.
        // This is the critical step that diverts new write commands to an in-memory buffer.
        let mut rewrite_state_guard = state.persistence.aof_rewrite_state.lock().await;
        if rewrite_state_guard.is_in_progress {
            warn!("AOF rewrite requested, but one is already in progress. Aborting.");
            return;
        }
        rewrite_state_guard.is_in_progress = true;
        // Take a consistent snapshot of all Lua scripts at the start of the rewrite.
        scripts_snapshot = state.scripting.get_all_scripts();
    }
    info!("AOF rewrite state set to 'in_progress'. New commands will be buffered.");

    let state_for_task = state.clone();
    // Spawn the I/O-heavy work onto a dedicated blocking thread to avoid starving the main Tokio runtime.
    let rewrite_task: JoinHandle<Result<(), SpinelDBError>> =
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            // We must block on the async part inside the blocking thread.
            rt.block_on(async move { do_rewrite_blocking(state_for_task, scripts_snapshot).await })
        });

    let rewrite_result = match rewrite_task.await {
        Ok(res) => res,
        Err(join_err) => Err(SpinelDBError::AofError(format!(
            "AOF rewrite task panicked: {join_err}"
        ))),
    };

    // Atomically take ownership of the buffered work that arrived during the rewrite.
    let buffered_work = {
        let mut rewrite_state_guard = state.persistence.aof_rewrite_state.lock().await;
        std::mem::take(&mut rewrite_state_guard.buffer)
    };

    if let Err(e) = rewrite_result {
        error!(
            "AOF rewrite failed: {}. Server will enter read-only mode. Buffered commands will be drained to the old AOF file by the writer task.",
            e
        );
        state.set_read_only(true, "AOF rewrite process failed");
    } else {
        info!("AOF rewrite temporary file created successfully.");
        // Append the buffered commands to the newly created file.
        if let Err(e) = append_buffered_work_to_temp_file(&state, buffered_work).await {
            error!(
                "Failed to append buffered work to new AOF file: {}. Aborting rewrite. Error: {}",
                state.config.lock().await.persistence.aof_path,
                e
            );
            state.set_read_only(true, "AOF rewrite process failed during buffer append");
        } else {
            info!("AOF rewrite process completed successfully.");
        }
    }

    // Signal the AOF writer task that the rewrite is complete (either in success or failure).
    // The writer will then handle the atomic file rename and drain any remaining buffer.
    if state.persistence.aof_rewrite_complete_tx.send(()).is_err() {
        error!(
            "Failed to send AOF rewrite completion signal. AOF writer might not switch to the new file or drain buffer."
        );
    } else {
        info!("Sent AOF rewrite completion signal to the writer task.");
    }
}

/// The core blocking logic that iterates through the entire database and writes its
/// state as a series of commands to a temporary file.
async fn do_rewrite_blocking(
    state: Arc<ServerState>,
    scripts_snapshot: HashMap<String, Bytes>,
) -> Result<(), SpinelDBError> {
    let aof_path = state.config.lock().await.persistence.aof_path.clone();
    let temp_file_path = get_temp_aof_path(&aof_path)?;
    // Use standard blocking file I/O as this runs on a blocking thread.
    let mut temp_file = StdFile::create(&temp_file_path)?;

    info!(
        "AOF rewrite: Writing database snapshot to temporary file: {:?}",
        temp_file_path
    );

    // First, write all loaded Lua scripts to make the new AOF self-contained.
    // This ensures EVALSHA commands can be replayed correctly.
    if !scripts_snapshot.is_empty() {
        info!(
            "AOF rewrite: Writing {} scripts to the new AOF.",
            scripts_snapshot.len()
        );
        let select_db_0: RespFrame =
            Command::Select(crate::core::commands::generic::Select { db_index: 0 }).into();
        temp_file.write_all(&select_db_0.encode_to_vec()?)?;
        for script_body in scripts_snapshot.values() {
            let script_load_cmd = Command::Script(ScriptCmd {
                subcommand: ScriptSubcommand::Load(script_body.clone()),
            });
            let frame: RespFrame = script_load_cmd.into();
            temp_file.write_all(&frame.encode_to_vec()?)?;
        }
    }

    // Iterate through each database and shard to write its state.
    for (db_index, db) in state.dbs.iter().enumerate() {
        if db.get_key_count() > 0 {
            // Write a SELECT command to switch to the correct database.
            let select_cmd: RespFrame =
                Command::Select(crate::core::commands::generic::Select { db_index }).into();
            temp_file.write_all(&select_cmd.encode_to_vec()?)?;

            // Iterate through each shard in the database.
            for shard_index in 0..crate::core::storage::db::NUM_SHARDS {
                let shard = db.get_shard(shard_index);
                let guard = shard.entries.lock().await;

                // Iterate through every key-value pair in the shard.
                for (key, stored_value) in guard.iter() {
                    // Skip keys that have already expired.
                    if stored_value.is_expired() {
                        continue;
                    }
                    // Convert the value back into a minimal set of construction commands.
                    write_value_as_commands(&mut temp_file, key, stored_value)?;
                }
            }
            info!("AOF rewrite: Snapshot of DB {} written.", db_index);
        }
    }

    // Ensure all buffered data is written to the OS.
    temp_file.sync_all()?;
    Ok(())
}

/// Appends commands that arrived during the rewrite to the new temporary AOF file.
/// This is the final step before the atomic rename.
async fn append_buffered_work_to_temp_file(
    state: &Arc<ServerState>,
    buffered_work: Vec<PropagatedWork>,
) -> Result<(), SpinelDBError> {
    if buffered_work.is_empty() {
        return Ok(());
    }

    let aof_path = &state.config.lock().await.persistence.aof_path;
    let temp_file_path = get_temp_aof_path(aof_path)?;
    let mut temp_file = std::fs::OpenOptions::new()
        .append(true)
        .open(&temp_file_path)?;

    info!(
        "AOF rewrite: Appending {} commands that arrived during snapshot to temp file.",
        buffered_work.len()
    );
    // CRITICAL: Get a fresh script snapshot to handle scripts loaded *during* the rewrite.
    let scripts_snapshot = state.scripting.get_all_scripts();
    for work_item in buffered_work {
        write_uow_to_file(&mut temp_file, work_item.uow, &scripts_snapshot)?;
    }
    temp_file.sync_all()?;
    Ok(())
}

/// Serializes a single StoredValue into the minimal set of commands needed to recreate it.
fn write_value_as_commands(
    file: &mut StdFile,
    key: &Bytes,
    stored_value: &StoredValue,
) -> Result<(), SpinelDBError> {
    // Delegate the complex serialization logic to the StoredValue itself.
    let commands = stored_value.to_construction_commands(key);
    for cmd in commands {
        let frame: RespFrame = cmd.into();
        file.write_all(&frame.encode_to_vec()?)?;
    }
    Ok(())
}

/// Serializes a UnitOfWork (a single command or a transaction) to a file.
fn write_uow_to_file(
    file: &mut StdFile,
    uow: UnitOfWork,
    scripts_snapshot: &HashMap<String, Bytes>,
) -> Result<(), SpinelDBError> {
    let frames_to_write: Vec<RespFrame> = match uow {
        UnitOfWork::Transaction(tx_data) => {
            let mut frames: Vec<RespFrame> = Vec::with_capacity(tx_data.all_commands.len() + 2);
            frames.push(Command::Multi.into());
            for cmd in tx_data.all_commands {
                frames.push(transform_evalsha_for_persistence(cmd, scripts_snapshot)?);
            }
            frames.push(Command::Exec.into());
            frames
        }
        UnitOfWork::Command(cmd) => {
            vec![transform_evalsha_for_persistence(*cmd, scripts_snapshot)?]
        }
    };
    for frame in frames_to_write {
        file.write_all(&frame.encode_to_vec()?)?;
    }
    Ok(())
}

/// Converts an `EVALSHA` command into an `EVAL` command for persistence, ensuring the AOF is self-contained.
/// This is crucial for data consistency on restore.
fn transform_evalsha_for_persistence(
    cmd: Command,
    scripts_snapshot: &HashMap<String, Bytes>,
) -> Result<RespFrame, SpinelDBError> {
    if let Command::EvalSha(evalsha_cmd) = cmd {
        // Look up the script body from the provided snapshot.
        if let Some(script_body) = scripts_snapshot.get(&evalsha_cmd.sha1) {
            Ok(Command::Eval(EvalCmd {
                script: script_body.clone(),
                num_keys: evalsha_cmd.num_keys,
                keys: evalsha_cmd.keys,
                args: evalsha_cmd.args,
            })
            .into())
        } else {
            // This case indicates a logic error where a script was executed but not found for persistence.
            let err_msg = format!(
                "Could not find script for EVALSHA {} during AOF rewrite. Cannot guarantee data consistency.",
                evalsha_cmd.sha1
            );
            error!("{}", err_msg);
            Err(SpinelDBError::AofError(err_msg))
        }
    } else {
        // For any other command, convert it to a frame directly.
        Ok(cmd.into())
    }
}

/// Generates the path for the temporary AOF file, e.g., "temp-rewrite-spineldb.aof".
fn get_temp_aof_path(original_path: &str) -> Result<PathBuf, SpinelDBError> {
    let path = Path::new(original_path);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| SpinelDBError::AofError("Invalid AOF path".into()))?;
    let temp_file_name = format!(
        "temp-rewrite-{}",
        file_name.to_str().unwrap_or("spineldb.aof")
    );
    Ok(parent.join(temp_file_name))
}
