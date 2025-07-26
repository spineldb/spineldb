// src/core/persistence/aof_writer.rs

//! Implements the Append-Only File (AOF) writer task.
//!
//! This task is responsible for writing commands that modify the dataset to the
//! AOF file on disk. It handles different `fsync` policies, a robust AOF rewrite
//! process, and graceful shutdown to ensure data durability.

use crate::config::AppendFsync;
use crate::core::events::{PropagatedWork, UnitOfWork};
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::{Command, SpinelDBError};
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::fs::{File as TokioFile, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{error, info, warn};

/// The number of retry attempts for a failed AOF write operation.
const AOF_RETRY_ATTEMPTS: u32 = 5;
/// The delay between AOF write retry attempts.
const AOF_RETRY_DELAY: Duration = Duration::from_secs(2);

/// The main struct for the AOF writer background task.
pub struct AofWriterTask {
    state: Arc<ServerState>,
    /// A buffered writer to the AOF file to improve performance.
    writer: BufWriter<TokioFile>,
    /// Receives work units (commands/transactions) from the EventBus.
    aof_event_rx: mpsc::Receiver<PropagatedWork>,
    /// Receives requests for periodic fsyncing (for `appendfsync = everysec`).
    fsync_request_rx: mpsc::Receiver<()>,
    /// A watch receiver to get notified when an AOF rewrite process is complete.
    aof_rewrite_complete_rx: watch::Receiver<()>,
}

impl AofWriterTask {
    /// Creates a new `AofWriterTask`. It takes ownership of the necessary channel receivers.
    pub async fn new(
        state: Arc<ServerState>,
        aof_event_rx: mpsc::Receiver<PropagatedWork>,
        fsync_request_rx: mpsc::Receiver<()>,
        aof_rewrite_complete_rx: watch::Receiver<()>,
    ) -> Result<Self, SpinelDBError> {
        let path = state.config.lock().await.persistence.aof_path.clone();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self {
            state,
            writer: BufWriter::new(file),
            aof_event_rx,
            fsync_request_rx,
            aof_rewrite_complete_rx,
        })
    }

    /// The main run loop for the AOF writer task.
    pub async fn run(
        mut self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), SpinelDBError> {
        let config = self.state.config.lock().await;
        info!(
            "AOF writer task started. Writing to {}. Fsync policy: {:?}",
            config.persistence.aof_path, config.persistence.appendfsync,
        );
        drop(config);

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    info!("AOF writer task shutting down. Performing final drain and sync.");
                    self.drain_and_sync_all().await?;
                    return Ok(());
                }
                Some(_) = self.fsync_request_rx.recv() => {
                    if let Err(e) = self.sync_to_disk().await {
                        error!("AOF fsync failed in periodic task: {}", e);
                    }
                }
                Ok(_) = self.aof_rewrite_complete_rx.changed() => {
                    if let Err(e) = self.handle_rewrite_completion().await {
                        let err_msg = format!("Critical error handling AOF rewrite completion: {e}. Entering read-only mode.");
                        error!("{}", err_msg);
                        self.state.set_read_only(true, "AOF rewrite completion failure");
                        return Err(SpinelDBError::AofError(err_msg));
                    }
                }
                maybe_work = self.aof_event_rx.recv() => {
                    match maybe_work {
                        Some(work) => {
                            self.handle_work_item(work).await?
                        }
                        None => {
                             info!("AOF channel closed, writer task shutting down.");
                             self.drain_and_sync_all().await?;
                             return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Drains all pending events from channels and performs a final sync before shutdown.
    async fn drain_and_sync_all(&mut self) -> Result<(), SpinelDBError> {
        self.aof_event_rx.close();
        while let Some(work) = self.aof_event_rx.recv().await {
            if let Err(e) = self.write_uow_to_file(&work.uow, false).await {
                warn!("Could not write pending AOF event during shutdown: {}", e);
            }
        }
        self.drain_rewrite_buffer(false).await?;
        if let Err(e) = self.sync_to_disk().await {
            error!("Failed to sync AOF file on shutdown: {}", e);
        }
        Ok(())
    }

    /// Handles a single work item by either buffering it or writing it to the AOF file.
    async fn handle_work_item(&mut self, work: PropagatedWork) -> Result<(), SpinelDBError> {
        let mut rewrite_state = self.state.persistence.aof_rewrite_state.lock().await;
        if rewrite_state.is_in_progress {
            rewrite_state.buffer.push(work);
            return Ok(());
        }
        drop(rewrite_state);

        self.write_uow_to_file(&work.uow, true).await?;

        let fsync_policy = self.state.config.lock().await.persistence.appendfsync;
        if fsync_policy == AppendFsync::Always {
            self.sync_to_disk().await?;
        }
        Ok(())
    }

    /// Manages the transition after an AOF rewrite is finished.
    async fn handle_rewrite_completion(&mut self) -> Result<(), SpinelDBError> {
        info!("AOF rewrite completed signal received. Handling transition.");
        let rewrite_succeeded = !self.state.is_read_only.load(Ordering::SeqCst);
        let aof_path = self.state.config.lock().await.persistence.aof_path.clone();

        self.drain_rewrite_buffer(rewrite_succeeded).await?;

        if rewrite_succeeded {
            if let Ok(metadata) = tokio::fs::metadata(&aof_path).await {
                self.state
                    .persistence
                    .aof_last_rewrite_size
                    .store(metadata.len(), Ordering::Relaxed);
                info!("Updated aof_last_rewrite_size to {} bytes.", metadata.len());
            } else {
                warn!("Could not read metadata of new AOF file to update rewrite size.");
            }
        }

        Ok(())
    }

    /// Drains the AOF rewrite buffer to the appropriate file.
    async fn drain_rewrite_buffer(
        &mut self,
        switch_to_new_file: bool,
    ) -> Result<(), SpinelDBError> {
        if switch_to_new_file {
            info!("AOF rewrite succeeded. Switching to new AOF file and draining buffer.");
            self.writer.flush().await?;
            self.writer.get_ref().sync_all().await.ok();

            let path = &self.state.config.lock().await.persistence.aof_path;
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?;
            self.writer = BufWriter::new(file);
            info!("Successfully switched to the new AOF file: {}", path);
        } else {
            warn!("AOF rewrite failed or shutdown initiated. Draining buffer to OLD AOF file.");
        }

        let buffered_work = {
            let mut rewrite_state = self.state.persistence.aof_rewrite_state.lock().await;
            rewrite_state.is_in_progress = false;
            std::mem::take(&mut rewrite_state.buffer)
        };
        info!("AOF rewrite state unlocked. Normal writing can resume.");

        if !buffered_work.is_empty() {
            info!(
                "Draining {} buffered commands from AOF rewrite process.",
                buffered_work.len()
            );
            for item in buffered_work {
                if let Err(e) = self.write_uow_to_file(&item.uow, false).await {
                    let err_msg = format!("Failed to write drained command to AOF: {e}.");
                    error!("{}", err_msg);
                    self.state.set_read_only(true, "AOF drain failure");
                    return Err(SpinelDBError::AofError(err_msg));
                }
            }
            self.sync_to_disk().await?;
            info!("AOF rewrite buffer successfully drained.");
        }
        Ok(())
    }

    /// Writes a `UnitOfWork` to the AOF file, with a retry mechanism for certain errors.
    async fn write_uow_to_file(
        &mut self,
        uow: &UnitOfWork,
        retry_on_fail: bool,
    ) -> Result<(), SpinelDBError> {
        let frames: Vec<RespFrame> = match uow {
            UnitOfWork::Transaction(tx_data) => {
                if tx_data.all_commands.is_empty() {
                    return Ok(());
                }
                let mut frames: Vec<RespFrame> = Vec::with_capacity(tx_data.all_commands.len() + 2);
                frames.push(Command::Multi.into());
                frames.extend(tx_data.all_commands.iter().cloned().map(Into::into));
                frames.push(Command::Exec.into());
                frames
            }
            UnitOfWork::Command(cmd) => vec![(**cmd).clone().into()],
        };

        for attempt in 0..=AOF_RETRY_ATTEMPTS {
            let mut encoded_bytes = Vec::new();
            for frame in &frames {
                encoded_bytes.extend_from_slice(&frame.encode_to_vec()?);
            }

            if encoded_bytes.is_empty() {
                return Ok(());
            }

            match self.writer.write_all(&encoded_bytes).await {
                Ok(_) => {
                    self.writer.flush().await?;
                    return Ok(());
                }
                Err(e) => {
                    if !retry_on_fail
                        || (e.kind() != ErrorKind::StorageFull
                            && e.kind() != ErrorKind::PermissionDenied)
                    {
                        let err_msg = format!("Unrecoverable AOF write error: {e}");
                        error!("{}", err_msg);
                        self.state.set_read_only(true, &err_msg);
                        return Err(e.into());
                    }

                    if attempt == AOF_RETRY_ATTEMPTS {
                        let err_msg =
                            format!("AOF write failed after {AOF_RETRY_ATTEMPTS} retries: {e}");
                        error!("{}", err_msg);
                        self.state
                            .set_read_only(true, "AOF write failure after multiple retries");
                        return Err(e.into());
                    }

                    warn!(
                        "AOF write failed: {}. Retrying in {:?} (Attempt {}/{})",
                        e,
                        AOF_RETRY_DELAY,
                        attempt + 1,
                        AOF_RETRY_ATTEMPTS
                    );
                    tokio::time::sleep(AOF_RETRY_DELAY).await;
                }
            }
        }
        unreachable!("AOF write loop finished without returning");
    }

    /// Flushes the OS buffer to disk (`fsync`).
    async fn sync_to_disk(&mut self) -> Result<(), SpinelDBError> {
        let is_rewriting = self
            .state
            .persistence
            .aof_rewrite_state
            .lock()
            .await
            .is_in_progress;
        if is_rewriting {
            return Ok(());
        }

        let start_time = Instant::now();

        if let Err(e) = self.writer.get_ref().sync_all().await {
            error!("Failed to fsync AOF file: {}. Entering read-only mode.", e);
            self.state.set_read_only(true, "AOF fsync failure");
            let latency = start_time.elapsed();
            self.state
                .latency_monitor
                .add_sample("aof-fsync", vec![], latency);
            return Err(e.into());
        }

        let latency = start_time.elapsed();
        self.state
            .latency_monitor
            .add_sample("aof-fsync", vec![], latency);
        Ok(())
    }
}
