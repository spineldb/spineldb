// src/core/commands/generic/client.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::state::ClientRole; // Import the new enum
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
    // Change: SetInfo struct now stores parsed data
    SetInfo {
        lib_name: Option<String>,
        lib_ver: Option<String>,
    },
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

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "list" => {
                // ... (no change)
                if args.len() > 1 {
                    return Err(SpinelDBError::WrongArgumentCount("CLIENT LIST".to_string()));
                }
                ClientSubcommand::List
            }
            "setname" => {
                // ... (no change)
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT SETNAME".to_string(),
                    ));
                }
                ClientSubcommand::SetName(extract_bytes(&args[1])?)
            }
            "getname" => {
                // ... (no change)
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT GETNAME".to_string(),
                    ));
                }
                ClientSubcommand::GetName
            }
            "kill" => {
                // ... (no change)
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount("CLIENT KILL".to_string()));
                }
                let id_to_kill = extract_string(&args[1])?
                    .parse::<u64>()
                    .map_err(|_| SpinelDBError::InvalidState("Invalid client ID".into()))?;
                ClientSubcommand::Kill(id_to_kill)
            }
            "setinfo" => {
                // --- NEW PARSING LOGIC FOR SETINFO ---
                if args.len() < 3 || args.len() % 2 != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT SETINFO".to_string(),
                    ));
                }
                let mut lib_name = None;
                let mut lib_ver = None;
                let mut i = 1;
                while i < args.len() {
                    let option = extract_string(&args[i])?.to_ascii_lowercase();
                    let value = extract_string(&args[i + 1])?;
                    match option.as_str() {
                        "lib-name" => lib_name = Some(value),
                        "lib-ver" => lib_ver = Some(value),
                        _ => { /* ignore other properties we don't recognize */ }
                    }
                    i += 2;
                }
                ClientSubcommand::SetInfo { lib_name, lib_ver }
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

                    // --- NEW FORMATTING FOR CLIENT LIST ---
                    let mut props = vec![
                        format!("id={}", client_info.session_id),
                        format!("addr={}", client_info.addr),
                        format!("age={}", client_info.created.elapsed().as_secs()),
                        format!("idle={}", client_info.last_command_time.elapsed().as_secs()),
                        format!("db={}", client_info.db_index),
                    ];
                    if let Some(name) = &client_info.name {
                        props.push(format!("name={name}"));
                    }
                    let role_str = match client_info.role {
                        ClientRole::Normal => "normal",
                        ClientRole::Replica => "replica",
                    };
                    props.push(format!("role={role_str}"));
                    if let Some(lib) = &client_info.library_name {
                        props.push(format!("lib-name={lib}"));
                    }
                    if let Some(ver) = &client_info.library_version {
                        props.push(format!("lib-ver={ver}"));
                    }

                    info_str.push_str(&props.join(" "));
                    info_str.push('\n');
                }
                Ok((
                    RespValue::BulkString(info_str.into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
            ClientSubcommand::SetName(name) => {
                // ... (no change)
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
                // ... (no change)
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
                // ... (no change)
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
            ClientSubcommand::SetInfo { lib_name, lib_ver } => {
                // --- NEW LOGIC TO SAVE INFO ---
                if let Some(entry) = ctx.state.clients.get(&ctx.session_id) {
                    let (client_info_arc, _) = entry.value();
                    let mut client_info = client_info_arc.lock().await;

                    if let Some(name) = lib_name {
                        client_info.library_name = Some(name.clone());
                    }
                    if let Some(ver) = lib_ver {
                        client_info.library_version = Some(ver.clone());
                    }

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
        }
    }
}
// ... (CommandSpec implementation does not need to be changed)
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
            ClientSubcommand::SetInfo { lib_name, lib_ver } => {
                args.push("SETINFO".into());
                if let Some(name) = lib_name {
                    args.extend_from_slice(&["LIB-NAME".into(), name.clone().into()]);
                }
                if let Some(ver) = lib_ver {
                    args.extend_from_slice(&["LIB-VER".into(), ver.clone().into()]);
                }
            }
        }
        args
    }
}
