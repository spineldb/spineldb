// src/core/commands/generic/config.rs

use crate::config::Config as ServerConfig;
use crate::core::cluster::gossip::{GossipMessage, GossipTaskMessage, now_ms};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use tracing::{error, info, warn};
use tracing_subscriber::filter::EnvFilter;

#[derive(Debug, Clone)]
pub enum ConfigSubcommand {
    Get(String),
    Set(String, String),
    Rewrite,
}

impl Default for ConfigSubcommand {
    fn default() -> Self {
        ConfigSubcommand::Get(String::new())
    }
}

/// A command for getting, setting, and rewriting server configuration.
/// Corresponds to the `CONFIG` SpinelDB command.
#[derive(Debug, Clone, Default)]
pub struct ConfigGetSet {
    pub subcommand: ConfigSubcommand,
}

impl ParseCommand for ConfigGetSet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CONFIG".to_string()));
        }

        let sub_str = match &args[0] {
            RespFrame::BulkString(bs) => String::from_utf8(bs.to_vec())
                .map_err(|_| SpinelDBError::WrongType)?
                .to_ascii_lowercase(),
            _ => return Err(SpinelDBError::WrongType),
        };
        let subcommand = match sub_str.as_str() {
            "get" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount("CONFIG GET".to_string()));
                }
                ConfigSubcommand::Get(extract_string(&args[1])?)
            }
            "set" => {
                if args.len() != 3 {
                    return Err(SpinelDBError::WrongArgumentCount("CONFIG SET".to_string()));
                }
                let param = extract_string(&args[1])?;
                let value = extract_string(&args[2])?;
                match param.to_lowercase().as_str() {
                    "maxmemory" | "loglevel" => ConfigSubcommand::Set(param, value),
                    _ => {
                        return Err(SpinelDBError::InvalidState(format!(
                            "Unsupported CONFIG SET parameter: {param}"
                        )));
                    }
                }
            }
            "rewrite" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CONFIG REWRITE".to_string(),
                    ));
                }
                ConfigSubcommand::Rewrite
            }
            _ => {
                return Err(SpinelDBError::UnknownCommand(
                    "CONFIG unknown subcommand".to_string(),
                ));
            }
        };

        Ok(ConfigGetSet { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for ConfigGetSet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            ConfigSubcommand::Get(param) => {
                let config = ctx.state.config.lock().await;
                let value = match param.to_lowercase().as_str() {
                    "databases" => Some(config.databases.to_string()),
                    "port" => Some(config.port.to_string()),
                    "host" => Some(config.host.clone()),
                    "maxmemory" => Some(config.maxmemory.unwrap_or(0).to_string()),
                    "aof_enabled" => Some(if config.persistence.aof_enabled {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    }),
                    "save" => {
                        let rules: Vec<String> = config
                            .persistence
                            .save_rules
                            .iter()
                            .map(|r| format!("{} {}", r.seconds, r.changes))
                            .collect();
                        Some(rules.join(" "))
                    }
                    "loglevel" => Some(config.log_level.clone()),
                    _ => None,
                };
                if let Some(val) = value {
                    Ok((
                        RespValue::Array(vec![
                            RespValue::BulkString(param.clone().into()),
                            RespValue::BulkString(val.into()),
                        ]),
                        WriteOutcome::DidNotWrite,
                    ))
                } else {
                    Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite))
                }
            }
            ConfigSubcommand::Set(param, value) => {
                // Apply the change locally first.
                let local_set_result = {
                    let mut config = ctx.state.config.lock().await;
                    match param.to_lowercase().as_str() {
                        "maxmemory" => {
                            let bytes: usize =
                                value.parse().map_err(|_| SpinelDBError::NotAnInteger)?;
                            config.maxmemory = if bytes == 0 { None } else { Some(bytes) };
                            Ok(())
                        }
                        "loglevel" => match EnvFilter::try_new(value) {
                            Ok(new_filter) => {
                                if let Err(e) = ctx.state.log_reload_handle.reload(new_filter) {
                                    let err_msg = format!("Failed to reload log level: {e}");
                                    error!("{err_msg}");
                                    Err(SpinelDBError::Internal(err_msg))
                                } else {
                                    config.log_level = value.clone();
                                    info!("Log level dynamically changed to '{}'", value);
                                    Ok(())
                                }
                            }
                            Err(e) => Err(SpinelDBError::InvalidState(format!(
                                "Invalid log filter directive: {e}"
                            ))),
                        },
                        _ => Err(SpinelDBError::InvalidState(format!(
                            "Unsupported CONFIG SET parameter: {param}"
                        ))),
                    }
                };

                // If local application succeeded, broadcast to the cluster.
                if local_set_result.is_ok()
                    && let Some(cluster) = &ctx.state.cluster
                {
                    info!("Broadcasting CONFIG SET {param} {value} to the cluster.");
                    let gossip_msg = GossipMessage::ConfigUpdate {
                        sender_id: cluster.my_id.clone(),
                        param: param.clone(),
                        value: value.clone(),
                        timestamp_ms: now_ms(),
                    };
                    let task_msg = GossipTaskMessage::Broadcast(gossip_msg);
                    if let Err(e) = ctx.state.cluster_gossip_tx.try_send(task_msg) {
                        warn!("Failed to broadcast CONFIG SET to cluster gossip task: {e}");
                    }
                }

                // Return the result of the local operation to the client.
                local_set_result.map(|()| {
                    (
                        RespValue::SimpleString("OK".into()),
                        WriteOutcome::DidNotWrite,
                    )
                })
            }
            ConfigSubcommand::Rewrite => {
                let config_clone: ServerConfig = {
                    let guard = ctx.state.config.lock().await;
                    guard.clone()
                };

                tokio::task::spawn_blocking(move || {
                    let mut config_to_write = config_clone;
                    config_to_write.password = None;

                    let toml_string = toml::to_string_pretty(&config_to_write)?;
                    std::fs::write("config.toml", toml_string)?;
                    Ok::<(), anyhow::Error>(())
                })
                .await
                .map_err(|e| SpinelDBError::Internal(format!("CONFIG REWRITE task failed: {e}")))?
                .map_err(|e| SpinelDBError::Internal(format!("Failed to write config: {e}")))?;

                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
        }
    }
}

impl CommandSpec for ConfigGetSet {
    fn name(&self) -> &'static str {
        "config"
    }
    fn arity(&self) -> i64 {
        -2
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
        let mut args = vec![];
        match &self.subcommand {
            ConfigSubcommand::Get(p) => args.extend_from_slice(&["GET".into(), p.clone().into()]),
            ConfigSubcommand::Set(p, v) => {
                args.extend_from_slice(&["SET".into(), p.clone().into(), v.clone().into()])
            }
            ConfigSubcommand::Rewrite => args.push("REWRITE".into()),
        }
        args
    }
}
