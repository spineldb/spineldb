// src/core/commands/generic/info.rs

use crate::config::ReplicationConfig;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

/// Implements the INFO command to provide server information and statistics.
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub section: Option<String>,
}

impl ParseCommand for Info {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        match args.len() {
            0 => Ok(Info { section: None }),
            1 => Ok(Info {
                section: Some(extract_string(&args[0])?.to_ascii_lowercase()),
            }),
            _ => Err(SpinelDBError::WrongArgumentCount("INFO".to_string())),
        }
    }
}

/// Gathers information from various parts of the server state.
async fn get_info_string(state: &ServerState, section: &Option<String>) -> String {
    let mut info = String::new();
    let all_sections = section.is_none() || section.as_deref() == Some("all");
    let config = state.config.lock().await;

    // Server Section
    if all_sections || section.as_deref() == Some("server") {
        info.push_str("# Server\r\n");
        info.push_str(&format!(
            "spineldb_version:{}\r\n",
            env!("CARGO_PKG_VERSION")
        ));
        info.push_str(&format!("tcp_port:{}\r\n", config.port));
        info.push_str("\r\n");
    }

    // Replication Section
    if all_sections || section.as_deref() == Some("replication") {
        info.push_str("# Replication\r\n");
        let role_str = match &config.replication {
            ReplicationConfig::Primary(_) => "master",
            ReplicationConfig::Replica { .. } => "slave",
        };
        info.push_str(&format!("role:{role_str}\r\n"));
        info.push_str(&format!(
            "master_replid:{}\r\n",
            state.replication.replication_info.master_replid
        ));
        info.push_str(&format!(
            "master_repl_offset:{}\r\n",
            state.replication.get_replication_offset()
        ));
        info.push_str(&format!(
            "connected_slaves:{}\r\n",
            state.replica_states.len()
        ));
        // Add min-replicas safety policy info if this is a primary.
        if let ReplicationConfig::Primary(primary_config) = &config.replication {
            info.push_str(&format!(
                "min_replicas_to_write:{}\r\n",
                primary_config.min_replicas_to_write
            ));
            info.push_str(&format!(
                "min_replicas_max_lag:{}\r\n",
                primary_config.min_replicas_max_lag
            ));
        }
        info.push_str("\r\n");
    }

    // Memory Section
    if all_sections || section.as_deref() == Some("memory") {
        info.push_str("# Memory\r\n");
        let used_memory: usize = state.dbs.iter().map(|db| db.get_current_memory()).sum();
        info.push_str(&format!("used_memory:{used_memory}\r\n"));
        info.push_str(&format!(
            "used_memory_human:{:.2}M\r\n",
            used_memory as f64 / (1024.0 * 1024.0)
        ));
        let max_memory = config.maxmemory.unwrap_or(0);
        info.push_str(&format!("maxmemory:{max_memory}\r\n"));
        info.push_str("\r\n");
    }

    // Persistence Section
    if all_sections || section.as_deref() == Some("persistence") {
        info.push_str("# Persistence\r\n");
        info.push_str(&format!(
            "aof_enabled:{}\r\n",
            if config.persistence.aof_enabled {
                "1"
            } else {
                "0"
            }
        ));
        let last_save_success_time = state.persistence.last_save_success_time.lock().await;
        let spldb_last_save_time_unix = if let Some(instant) = *last_save_success_time {
            let duration_since_save = instant.elapsed().as_secs();
            chrono::Utc::now().timestamp() - duration_since_save as i64
        } else {
            0 // No successful save yet.
        };

        info.push_str(&format!(
            "spldb_last_save_time:{spldb_last_save_time_unix}\r\n"
        ));
        info.push_str(&format!(
            "spldb_bgsave_in_progress:{}\r\n",
            if state.persistence.is_saving_spldb.load(Ordering::Relaxed) {
                "1"
            } else {
                "0"
            }
        ));
        let aof_in_progress = state
            .persistence
            .aof_rewrite_state
            .try_lock()
            .map(|guard| guard.is_in_progress)
            .unwrap_or(true);
        info.push_str(&format!(
            "aof_rewrite_in_progress:{}\r\n",
            if aof_in_progress { "1" } else { "0" }
        ));
        info.push_str("\r\n");
    }

    // Stats Section
    if all_sections || section.as_deref() == Some("stats") {
        info.push_str("# Stats\r\n");
        info.push_str(&format!(
            "total_connections_received:{}\r\n",
            state.stats.get_total_connections()
        ));
        info.push_str(&format!(
            "total_commands_processed:{}\r\n",
            state.stats.get_total_commands()
        ));
        // Add new metric for monitoring lazy-free task health.
        info.push_str(&format!(
            "lazy_free_queue_full_errors:{}\r\n",
            state.persistence.get_lazy_free_errors()
        ));
        info.push_str("\r\n");
    }

    info
}

#[async_trait]
impl ExecutableCommand for Info {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let info_string = get_info_string(&ctx.state, &self.section).await;
        Ok((
            RespValue::BulkString(info_string.into()),
            WriteOutcome::DidNotWrite,
        ))
    }
}
impl CommandSpec for Info {
    fn name(&self) -> &'static str {
        "info"
    }
    fn arity(&self) -> i64 {
        -1
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE | CommandFlags::READONLY
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
        self.section.clone().map_or(vec![], |s| vec![s.into()])
    }
}
