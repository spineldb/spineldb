// src/core/commands/cluster/meet.rs

use crate::core::cluster::gossip::{GossipMessage, GossipTaskMessage, now_ms};
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use std::net::{SocketAddr, ToSocketAddrs};
use tracing::{info, warn};

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    ip: &str,
    port: u16,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    info!("CLUSTER MEET initiated for {}:{}", ip, port);

    let bus_port_offset = ctx.state.config.lock().await.cluster.bus_port_offset;

    let bus_port = u32::from(port)
        .checked_add(u32::from(bus_port_offset))
        .and_then(|p| u16::try_from(p).ok())
        .ok_or(SpinelDBError::InvalidState(
            "Invalid target port for cluster bus".into(),
        ))?;

    let target_addr_str = format!("{ip}:{bus_port}");

    let target_addr: SocketAddr = target_addr_str.to_socket_addrs()?.next().ok_or_else(|| {
        SpinelDBError::Internal(format!("Could not resolve address: {target_addr_str}"))
    })?;

    let meet_msg = GossipMessage::Meet {
        timestamp_ms: now_ms(),
    };
    let task_msg = GossipTaskMessage::DirectSend {
        message: meet_msg,
        target: target_addr,
    };

    if let Err(e) = ctx.state.cluster_gossip_tx.try_send(task_msg) {
        warn!("Failed to send CLUSTER MEET to gossip worker: {}", e);
        return Err(SpinelDBError::Internal(
            "Failed to communicate with gossip worker".into(),
        ));
    }

    Ok((
        RespValue::SimpleString("OK".into()),
        WriteOutcome::DidNotWrite,
    ))
}
