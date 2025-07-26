// src/core/commands/generic/role.rs

//! Implements the `ROLE` command, which provides information about the
//! server's current role in replication (primary or replica) and its state.

use crate::config::ReplicationConfig;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::validate_arg_count;
use crate::core::protocol::RespFrame;
use crate::core::state::ReplicaSyncState;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Represents the `ROLE` command.
#[derive(Debug, Clone, Default)]
pub struct Role;

impl ParseCommand for Role {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "ROLE")?;
        Ok(Role)
    }
}

#[async_trait]
impl ExecutableCommand for Role {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let config = ctx.state.config.lock().await;

        let role_info = match &config.replication {
            // If this server is a primary...
            ReplicationConfig::Primary(_) => {
                let mut replicas_info_array = vec![];
                // Iterate through connected replicas to build their status array.
                for entry in ctx.state.replica_states.iter() {
                    let addr = entry.key();
                    let info = entry.value();
                    if info.sync_state == ReplicaSyncState::Online {
                        replicas_info_array.push(RespValue::Array(vec![
                            // Replica IP
                            RespValue::BulkString(addr.ip().to_string().into()),
                            // Replica Port
                            RespValue::BulkString(addr.port().to_string().into()),
                            // Replica's acknowledged offset
                            RespValue::BulkString(info.ack_offset.to_string().into()),
                        ]));
                    }
                }

                // Format the response for a primary node.
                RespValue::Array(vec![
                    // Role: "master" (for SpinelDB compatibility)
                    RespValue::BulkString("master".into()),
                    // Current replication offset
                    RespValue::Integer(ctx.state.replication.get_replication_offset() as i64),
                    // Array of connected replicas
                    RespValue::Array(replicas_info_array),
                ])
            }
            // If this server is a replica...
            ReplicationConfig::Replica {
                primary_host,
                primary_port,
                ..
            } => {
                let replica_info = ctx.state.replication.replica_info.lock().await;
                let (conn_state, offset) = if let Some(info) = replica_info.as_ref() {
                    // If connected, show "connected" and the processed offset.
                    ("connected", info.processed_offset)
                } else {
                    // Otherwise, show "connecting".
                    ("connecting", 0)
                };

                // Format the response for a replica node.
                RespValue::Array(vec![
                    // Role: "slave" (for SpinelDB compatibility)
                    RespValue::BulkString("slave".into()),
                    // Primary's host
                    RespValue::BulkString(primary_host.clone().into()),
                    // Primary's port
                    RespValue::Integer(*primary_port as i64),
                    // Connection state
                    RespValue::BulkString(conn_state.into()),
                    // Processed replication offset
                    RespValue::Integer(offset as i64),
                ])
            }
        };

        Ok((role_info, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Role {
    fn name(&self) -> &'static str {
        "role"
    }

    fn arity(&self) -> i64 {
        1
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::NO_PROPAGATE
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
