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
    SetInfo {
        lib_name: Option<String>,
        lib_ver: Option<String>,
    },
    Id,
    NoEvict(bool),
    NoTouch(bool),
    Tracking {
        enabled: bool,
        redirect: Option<u64>,
        bcast: bool,
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
            "id" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount("CLIENT ID".to_string()));
                }
                ClientSubcommand::Id
            }
            "no-evict" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT NO-EVICT".to_string(),
                    ));
                }
                let flag = extract_string(&args[1])?.to_ascii_lowercase();
                match flag.as_str() {
                    "on" => ClientSubcommand::NoEvict(true),
                    "off" => ClientSubcommand::NoEvict(false),
                    _ => return Err(SpinelDBError::SyntaxError),
                }
            }
            "no-touch" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT NO-TOUCH".to_string(),
                    ));
                }
                let flag = extract_string(&args[1])?.to_ascii_lowercase();
                match flag.as_str() {
                    "on" => ClientSubcommand::NoTouch(true),
                    "off" => ClientSubcommand::NoTouch(false),
                    _ => return Err(SpinelDBError::SyntaxError),
                }
            }
            "tracking" => {
                if args.len() < 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLIENT TRACKING".to_string(),
                    ));
                }
                let enabled_str = extract_string(&args[1])?.to_ascii_lowercase();
                let enabled = match enabled_str.as_str() {
                    "on" => true,
                    "off" => false,
                    _ => return Err(SpinelDBError::SyntaxError),
                };
                let mut redirect = None;
                let mut bcast = false;
                let mut i = 2;
                while i < args.len() {
                    let opt = extract_string(&args[i])?.to_ascii_lowercase();
                    match opt.as_str() {
                        "redirect" => {
                            if i + 1 < args.len() {
                                redirect = Some(
                                    extract_string(&args[i + 1])?.parse::<u64>().map_err(|_| {
                                        SpinelDBError::InvalidState(
                                            "Invalid client ID for REDIRECT".into(),
                                        )
                                    })?,
                                );
                                i += 2;
                            } else {
                                return Err(SpinelDBError::SyntaxError);
                            }
                        }
                        "bcast" => {
                            bcast = true;
                            i += 1;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
                ClientSubcommand::Tracking {
                    enabled,
                    redirect,
                    bcast,
                }
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
                        format!("proto={}", client_info.protocol_version),
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
            ClientSubcommand::Id => Ok((
                RespValue::Integer(ctx.session_id as i64),
                WriteOutcome::DidNotWrite,
            )),
            ClientSubcommand::NoEvict(enabled) => {
                if let Some(entry) = ctx.state.clients.get(&ctx.session_id) {
                    let (client_info_arc, _) = entry.value();
                    client_info_arc.lock().await.no_evict = *enabled;
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
            ClientSubcommand::NoTouch(enabled) => {
                if let Some(entry) = ctx.state.clients.get(&ctx.session_id) {
                    let (client_info_arc, _) = entry.value();
                    client_info_arc.lock().await.no_touch = *enabled;
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
            ClientSubcommand::Tracking {
                enabled,
                redirect,
                bcast,
            } => {
                if *enabled {
                    // Enable tracking
                    if let Some(_redirect_id) = redirect {
                        // Redirect mode: invalidation messages go to the redirect client
                        // For now, we support it conceptually but don't implement the
                        // redirect channel yet (requires a separate client to receive).
                        // We still enable tracking on this session.
                    }

                    ctx.state.tracking.enable_tracking(ctx.session_id);

                    if *bcast {
                        // BCAST mode: we'll register keys as they are accessed
                        // For now, just mark the session as bcast-enabled
                    }

                    Ok((
                        RespValue::SimpleString("OK".into()),
                        WriteOutcome::DidNotWrite,
                    ))
                } else {
                    // Disable tracking
                    ctx.state.tracking.disable_tracking(ctx.session_id);
                    Ok((
                        RespValue::SimpleString("OK".into()),
                        WriteOutcome::DidNotWrite,
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
            ClientSubcommand::Id => args.push("ID".into()),
            ClientSubcommand::NoEvict(enabled) => {
                args.extend_from_slice(&[
                    "NO-EVICT".into(),
                    if *enabled { "ON" } else { "OFF" }.into(),
                ]);
            }
            ClientSubcommand::NoTouch(enabled) => {
                args.extend_from_slice(&[
                    "NO-TOUCH".into(),
                    if *enabled { "ON" } else { "OFF" }.into(),
                ]);
            }
            ClientSubcommand::Tracking {
                enabled,
                redirect,
                bcast,
            } => {
                args.push("TRACKING".into());
                args.push(if *enabled { "ON" } else { "OFF" }.into());
                if *bcast {
                    args.push("BCAST".into());
                }
                if let Some(id) = redirect {
                    args.extend_from_slice(&["REDIRECT".into(), id.to_string().into()]);
                }
            }
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &'static str) -> RespFrame {
        RespFrame::BulkString(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn test_client_parse_list() -> Result<(), SpinelDBError> {
        let c = Client::parse(&[bs("list")]).unwrap();
        assert!(matches!(c.subcommand, ClientSubcommand::List));
        Ok(())
    }

    #[test]
    fn test_client_parse_setname() -> Result<(), SpinelDBError> {
        let c = Client::parse(&[bs("setname"), bs("myclient")]).unwrap();
        if let ClientSubcommand::SetName(n) = &c.subcommand {
            assert_eq!(n.as_ref(), b"myclient");
        } else {
            panic!("Expected SetName subcommand");
        }
        Ok(())
    }

    #[test]
    fn test_client_parse_getname() -> Result<(), SpinelDBError> {
        let c = Client::parse(&[bs("getname")]).unwrap();
        assert!(matches!(c.subcommand, ClientSubcommand::GetName));
        Ok(())
    }

    #[test]
    fn test_client_parse_kill() -> Result<(), SpinelDBError> {
        let c = Client::parse(&[bs("kill"), bs("123")]).unwrap();
        assert!(matches!(c.subcommand, ClientSubcommand::Kill(123)));
        Ok(())
    }

    #[test]
    fn test_client_parse_kill_invalid_id() {
        let r = Client::parse(&[bs("kill"), bs("not_a_number")]);
        assert!(r.is_err());
    }

    #[test]
    fn test_client_parse_setinfo() -> Result<(), SpinelDBError> {
        let c = Client::parse(&[
            bs("setinfo"),
            bs("LIB-NAME"),
            bs("mylib"),
            bs("LIB-VER"),
            bs("1.0"),
        ])
        .unwrap();
        if let ClientSubcommand::SetInfo { lib_name, lib_ver } = &c.subcommand {
            assert_eq!(lib_name, &Some("mylib".to_string()));
            assert_eq!(lib_ver, &Some("1.0".to_string()));
        } else {
            panic!("Expected SetInfo subcommand");
        }
        Ok(())
    }

    #[test]
    fn test_client_parse_setinfo_invalid_arg_count() {
        let r = Client::parse(&[bs("setinfo"), bs("LIB-NAME")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_client_parse_no_args() {
        let r = Client::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_client_parse_unknown_subcommand() {
        let r = Client::parse(&[bs("unknown")]);
        assert!(matches!(r, Err(SpinelDBError::UnknownCommand(_))));
    }

    #[test]
    fn test_client_to_resp_args_list() -> Result<(), SpinelDBError> {
        let c = Client::parse(&[bs("list")])?;
        let args = c.to_resp_args();
        assert_eq!(args, vec![Bytes::from_static(b"LIST")]);
        Ok(())
    }
}
