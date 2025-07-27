// src/core/commands/generic/client.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::state::ClientRole; // Import the new enum
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub enum ClientSubcommand {
    #[default]
    List,
    SetName(Bytes),
    GetName,
    Kill(u64),
    SetInfo, // For client library compatibility (e.g., redis-py)
}

#[derive(Debug, Clone, Default)]
pub struct Client {
    pub subcommand: ClientSubcommand,
}

impl ParseCommand for Client {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CLIENT".to_string()));
        }

        let sub_str = match &args[0] {
            RespFrame::BulkString(bs) => String::from_utf8(bs.to_vec())
                .map_err(|_| SpinelDBError::WrongType)?
                .to_ascii_lowercase(),
            _ => return Err(SpinelDBError::WrongType),
        };
        let subcommand = match sub_str.as_str() {
            "list" => {
                if args.len() > 1 {
                    return Err(SpinelDBError::WrongArgumentCount("CLIENT LIST".to_string()));
                }
                ClientSubcommand::List
            }
            "setname" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT SETNAME".to_string(),
                    ));
                }
                ClientSubcommand::SetName(extract_bytes(&args[1])?)
            }
            "getname" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT GETNAME".to_string(),
                    ));
                }
                ClientSubcommand::GetName
            }
            "kill" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount("CLIENT KILL".to_string()));
                }
                let id_to_kill = extract_string(&args[1])?
                    .parse::<u64>()
                    .map_err(|_| SpinelDBError::InvalidState("Invalid client ID".into()))?;
                ClientSubcommand::Kill(id_to_kill)
            }
            "setinfo" => {
                // Handle CLIENT SETINFO LIB-NAME <name> and LIB-VER <ver>
                if args.len() != 3 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT SETINFO".to_string(),
                    ));
                }
                ClientSubcommand::SetInfo
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("CLIENT {sub_str}"))),
        };

        Ok(Client { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Client {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            ClientSubcommand::List => {
                let mut info_str = String::new();
                for entry in ctx.state.clients.iter() {
                    let (client_info_arc, _) = entry.value();
                    let client_info = client_info_arc.lock().await;
                    let name = client_info.name.as_deref().unwrap_or("");
                    let age = client_info.created.elapsed().as_secs();
                    let idle = client_info.last_command_time.elapsed().as_secs();

                    // Format the client's role for display.
                    let role_str = match client_info.role {
                        ClientRole::Normal => "normal",
                        ClientRole::Replica => "replica",
                    };

                    info_str.push_str(&format!(
                        "id={} addr={} name={} age={} idle={} db={} role={}\n",
                        client_info.session_id,
                        client_info.addr,
                        name,
                        age,
                        idle,
                        client_info.db_index,
                        role_str
                    ));
                }
                Ok((
                    RespValue::BulkString(info_str.into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            ClientSubcommand::SetName(name) => {
                if let Some(entry) = ctx.state.clients.get(&ctx.session_id) {
                    let (client_info_arc, _) = entry.value();
                    client_info_arc.lock().await.name =
                        Some(String::from_utf8_lossy(name).to_string());
                    Ok((
                        RespValue::SimpleString("OK".into()),
                        WriteOutcome::DidNotWrite,
                    ))
                } else {
                    Err(SpinelDBError::Internal(
                        "Client not found in registry".into(),
                    ))
                }
            }
            ClientSubcommand::GetName => {
                if let Some(entry) = ctx.state.clients.get(&ctx.session_id) {
                    let (client_info_arc, _) = entry.value();
                    let name = client_info_arc.lock().await.name.clone();
                    Ok((
                        name.map(|n| RespValue::BulkString(n.into()))
                            .unwrap_or(RespValue::Null),
                        WriteOutcome::DidNotWrite,
                    ))
                } else {
                    Err(SpinelDBError::Internal(
                        "Client not found in registry".into(),
                    ))
                }
            }
            ClientSubcommand::Kill(id_to_kill) => {
                if let Some(entry) = ctx.state.clients.get(id_to_kill) {
                    let (_, shutdown_tx) = entry.value();
                    let _ = shutdown_tx.send(());
                    Ok((
                        RespValue::SimpleString("OK".into()),
                        WriteOutcome::DidNotWrite,
                    ))
                } else {
                    Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
                }
            }
            ClientSubcommand::SetInfo => {
                // Implemented as a no-op for client library compatibility.
                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
        }
    }
}

impl CommandSpec for Client {
    fn name(&self) -> &'static str {
        "client"
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
            ClientSubcommand::List => args.push("LIST".into()),
            ClientSubcommand::SetName(name) => {
                args.extend_from_slice(&["SETNAME".into(), name.clone()])
            }
            ClientSubcommand::GetName => args.push("GETNAME".into()),
            ClientSubcommand::Kill(id) => {
                args.extend_from_slice(&["KILL".into(), id.to_string().into()])
            }
            ClientSubcommand::SetInfo => args.push("SETINFO".into()),
        }
        args
    }
}
