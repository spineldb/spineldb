// src/core/commands/generic/flushall.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::validate_arg_count;
use crate::core::protocol::{RespFrame, RespFrameCodec};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use std::sync::atomic::Ordering;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::info;

/// Represents the `FLUSHALL` command.
#[derive(Debug, Clone, Default)]
pub struct FlushAll;

impl ParseCommand for FlushAll {
    /// Parses the arguments for the FLUSHALL command.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "FLUSHALL")?;
        Ok(FlushAll)
    }
}

#[async_trait]
impl ExecutableCommand for FlushAll {
    /// Executes the FLUSHALL command. In cluster mode, it broadcasts FLUSHDB to all
    /// primary nodes and waits for their completion before flushing the local data.
    /// In standalone mode, it flushes all local databases.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Prevent FLUSHALL during an ongoing background save to avoid data inconsistency.
        if ctx
            .state
            .persistence
            .is_saving_spldb
            .load(Ordering::Relaxed)
        {
            return Err(SpinelDBError::InvalidState(
                "Cannot FLUSHALL when a background save is in progress".into(),
            ));
        }

        // In cluster mode, broadcast FLUSHDB to all other primary nodes first.
        if let Some(cluster_state) = &ctx.state.cluster {
            info!("FLUSHALL received in cluster mode. Broadcasting FLUSHDB to all masters.");

            let password = ctx.state.config.lock().await.password.clone();
            let flushdb_cmd_frame = RespFrame::Array(vec![RespFrame::BulkString("FLUSHDB".into())]);

            // Collect all broadcast tasks to await their results.
            let mut tasks: Vec<JoinHandle<Result<(), SpinelDBError>>> = Vec::new();

            for node_entry in cluster_state.nodes.iter() {
                let node_info = &node_entry.value().node_info;

                // Skip self and non-primary nodes.
                if !node_info
                    .get_flags()
                    .contains(crate::core::cluster::NodeFlags::PRIMARY)
                    || node_info
                        .get_flags()
                        .contains(crate::core::cluster::NodeFlags::MYSELF)
                {
                    continue;
                }

                let addr_str = node_info.addr.clone();
                let password_clone = password.clone();
                let flushdb_frame_clone = flushdb_cmd_frame.clone();

                let handle = tokio::spawn(async move {
                    let stream = TcpStream::connect(&addr_str).await.map_err(|e| {
                        SpinelDBError::Internal(format!(
                            "FLUSHALL: Failed to connect to master {addr_str}: {e}"
                        ))
                    })?;

                    let mut framed = Framed::new(stream, RespFrameCodec);

                    if let Some(pass) = password_clone {
                        let auth_cmd_frame = RespFrame::Array(vec![
                            RespFrame::BulkString("AUTH".into()),
                            RespFrame::BulkString(pass.into()),
                        ]);
                        framed.send(auth_cmd_frame).await.map_err(|e| {
                            SpinelDBError::Internal(format!(
                                "FLUSHALL: Failed to send AUTH to master {addr_str}: {e}"
                            ))
                        })?;
                    }

                    framed.send(flushdb_frame_clone).await.map_err(|e| {
                        SpinelDBError::Internal(format!(
                            "FLUSHALL: Failed to send FLUSHDB to master {addr_str}: {e}"
                        ))
                    })?;

                    info!("FLUSHALL: Sent FLUSHDB to master {}", addr_str);
                    Ok(())
                });
                tasks.push(handle);
            }

            // Await all broadcast tasks. If any fail, the entire operation fails.
            for handle in tasks {
                match handle.await {
                    Ok(Ok(_)) => { /* Success */ }
                    Ok(Err(e)) => {
                        return Err(SpinelDBError::Internal(format!(
                            "Failed to flush a master node: {e}"
                        )));
                    }
                    Err(e) => {
                        return Err(SpinelDBError::Internal(format!(
                            "FLUSHALL broadcast task panicked: {e}"
                        )));
                    }
                }
            }
        }

        // Flush all local databases.
        info!("Flushing all local databases.");
        for db in &ctx.state.dbs {
            let guards = db.lock_all_shards().await;
            for mut guard in guards {
                guard.clear();
            }
        }

        ctx.state
            .persistence
            .dirty_keys_counter
            .store(0, Ordering::Relaxed);
        Ok((RespValue::SimpleString("OK".into()), WriteOutcome::Flush))
    }
}

impl CommandSpec for FlushAll {
    fn name(&self) -> &'static str {
        "flushall"
    }

    fn arity(&self) -> i64 {
        1
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::NO_PROPAGATE
    }

    fn first_key(&self) -> i64 {
        0
    }

    fn last_key(&self) -> i64 {
        0
    }

    fn step(&self) -> i64 {
        0
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![]
    }
}
