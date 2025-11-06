// src/core/commands/cluster/mod.rs

//! Implements the `CLUSTER` command dispatcher and its various subcommands.

// Declare all submodule files.
mod addslots;
mod fix;
mod forget;
mod getkeysinslot;
mod meet;
mod nodes;
mod replicate;
mod reshard;
mod setslot;
mod slots;

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

/// An enum representing all supported `CLUSTER` subcommands.
#[derive(Debug, Clone, Default)]
pub enum ClusterSubcommand {
    #[default]
    Nodes,
    Slots,
    MyId,
    AddSlots(Vec<u16>),
    GetKeysInSlot {
        slot: u16,
        count: usize,
    },
    Meet(String, u16),
    SetSlot(u16, SetSlotSubcommand),
    Replicate(String),
    Reshard {
        source_node_id: String,
        destination_node_id: String,
        slots: Vec<u16>,
    },
    Forget(String),
    Fix,
}

/// An enum for the sub-options of the `CLUSTER SETSLOT` command.
#[derive(Debug, Clone)]
pub enum SetSlotSubcommand {
    Migrating(String), // destination node_id
    Importing(String), // source node_id
    Node(String),      // new owner node_id
    Stable,
}

/// The main struct for the `CLUSTER` command.
#[derive(Debug, Clone, Default)]
pub struct ClusterInfo {
    pub subcommand: ClusterSubcommand,
}

impl ParseCommand for ClusterInfo {
    /// Parses the arguments for the CLUSTER command and its various subcommands.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("CLUSTER".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "nodes" => ClusterSubcommand::Nodes,
            "slots" => ClusterSubcommand::Slots,
            "myid" => ClusterSubcommand::MyId,
            "getkeysinslot" => {
                if args.len() != 3 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER GETKEYSINSLOT".to_string(),
                    ));
                }
                let slot = extract_string(&args[1])?.parse::<u16>()?;
                let count = extract_string(&args[2])?.parse::<usize>()?;
                ClusterSubcommand::GetKeysInSlot { slot, count }
            }
            "addslots" => {
                if args.len() < 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER ADDSLOTS".to_string(),
                    ));
                }
                let slots = args[1..]
                    .iter()
                    .map(|f| {
                        extract_string(f)?
                            .parse::<u16>()
                            .map_err(|_| SpinelDBError::NotAnInteger)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                ClusterSubcommand::AddSlots(slots)
            }
            "meet" => {
                if args.len() != 3 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER MEET".to_string(),
                    ));
                }
                let ip = extract_string(&args[1])?;
                let port = extract_string(&args[2])?.parse::<u16>()?;
                ClusterSubcommand::Meet(ip, port)
            }
            "setslot" => {
                if args.len() < 3 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER SETSLOT".to_string(),
                    ));
                }
                let slot: u16 = extract_string(&args[1])?.parse()?;
                let subsub_cmd_str = extract_string(&args[2])?.to_ascii_lowercase();
                let subsub_cmd = match subsub_cmd_str.as_str() {
                    "migrating" | "importing" | "node" if args.len() != 4 => {
                        Err(SpinelDBError::SyntaxError)
                    }
                    "stable" if args.len() != 3 => Err(SpinelDBError::SyntaxError),
                    "migrating" => Ok(SetSlotSubcommand::Migrating(extract_string(&args[3])?)),
                    "importing" => Ok(SetSlotSubcommand::Importing(extract_string(&args[3])?)),
                    "node" => Ok(SetSlotSubcommand::Node(extract_string(&args[3])?)),
                    "stable" => Ok(SetSlotSubcommand::Stable),
                    _ => Err(SpinelDBError::SyntaxError),
                }?;
                ClusterSubcommand::SetSlot(slot, subsub_cmd)
            }
            "replicate" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER REPLICATE".to_string(),
                    ));
                }
                ClusterSubcommand::Replicate(extract_string(&args[1])?)
            }
            "reshard" => {
                if args.len() < 4 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER RESHARD".to_string(),
                    ));
                }
                let source_node_id = extract_string(&args[1])?;
                let destination_node_id = extract_string(&args[2])?;
                let slots: Vec<u16> = args[3..]
                    .iter()
                    .map(|f| {
                        extract_string(f)?
                            .parse::<u16>()
                            .map_err(|_| SpinelDBError::NotAnInteger)
                    })
                    .collect::<Result<_, _>>()?;
                if slots.is_empty() {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER RESHARD".to_string(),
                    ));
                }
                ClusterSubcommand::Reshard {
                    source_node_id,
                    destination_node_id,
                    slots,
                }
            }
            "forget" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "CLUSTER FORGET".to_string(),
                    ));
                }
                ClusterSubcommand::Forget(extract_string(&args[1])?)
            }
            "fix" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount("CLUSTER FIX".to_string()));
                }
                ClusterSubcommand::Fix
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("CLUSTER {sub_str}"))),
        };
        Ok(ClusterInfo { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for ClusterInfo {
    /// Dispatches the command to the appropriate subcommand's execution logic.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let cluster = ctx
            .state
            .cluster
            .as_ref()
            .ok_or(SpinelDBError::InvalidState(
                "Cluster mode is not enabled.".into(),
            ))?;

        match &self.subcommand {
            ClusterSubcommand::MyId => Ok((
                RespValue::BulkString(cluster.my_id.clone().into()),
                WriteOutcome::DidNotWrite,
            )),
            ClusterSubcommand::Nodes => nodes::execute(ctx).await,
            ClusterSubcommand::Slots => slots::execute(ctx).await,
            ClusterSubcommand::GetKeysInSlot { slot, count } => {
                getkeysinslot::execute(ctx, *slot, *count).await
            }
            ClusterSubcommand::AddSlots(slots) => addslots::execute(ctx, slots).await,
            ClusterSubcommand::Meet(ip, port) => meet::execute(ctx, ip, *port).await,
            ClusterSubcommand::SetSlot(slot, subcmd) => setslot::execute(ctx, *slot, subcmd).await,
            ClusterSubcommand::Replicate(master_id) => replicate::execute(ctx, master_id).await,
            ClusterSubcommand::Reshard {
                source_node_id,
                destination_node_id,
                slots,
            } => reshard::execute(ctx, source_node_id, destination_node_id, slots).await,
            ClusterSubcommand::Forget(node_id) => forget::execute(ctx, node_id).await,
            ClusterSubcommand::Fix => fix::execute(ctx).await,
        }
    }
}

impl CommandSpec for ClusterInfo {
    fn name(&self) -> &'static str {
        "cluster"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE
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
        match &self.subcommand {
            ClusterSubcommand::Nodes => vec!["NODES".into()],
            ClusterSubcommand::Slots => vec!["SLOTS".into()],
            ClusterSubcommand::MyId => vec!["MYID".into()],
            ClusterSubcommand::GetKeysInSlot { slot, count } => vec![
                "GETKEYSINSLOT".into(),
                slot.to_string().into(),
                count.to_string().into(),
            ],
            ClusterSubcommand::AddSlots(slots) => {
                let mut args = vec!["ADDSLOTS".into()];
                args.extend(slots.iter().map(|s| s.to_string().into()));
                args
            }
            ClusterSubcommand::Meet(ip, port) => {
                vec!["MEET".into(), ip.clone().into(), port.to_string().into()]
            }
            ClusterSubcommand::SetSlot(slot, subcmd) => {
                let mut args = vec!["SETSLOT".into(), slot.to_string().into()];
                match subcmd {
                    SetSlotSubcommand::Node(id) => args.extend(["NODE".into(), id.clone().into()]),
                    SetSlotSubcommand::Migrating(id) => {
                        args.extend(["MIGRATING".into(), id.clone().into()])
                    }
                    SetSlotSubcommand::Importing(id) => {
                        args.extend(["IMPORTING".into(), id.clone().into()])
                    }
                    SetSlotSubcommand::Stable => args.push("STABLE".into()),
                };
                args
            }
            ClusterSubcommand::Replicate(master_id) => {
                vec!["REPLICATE".into(), master_id.clone().into()]
            }
            ClusterSubcommand::Reshard {
                source_node_id,
                destination_node_id,
                slots,
            } => {
                let mut args = vec![
                    "RESHARD".into(),
                    source_node_id.clone().into(),
                    destination_node_id.clone().into(),
                ];
                args.extend(slots.iter().map(|s| s.to_string().into()));
                args
            }
            ClusterSubcommand::Forget(node_id) => {
                vec!["FORGET".into(), node_id.clone().into()]
            }
            ClusterSubcommand::Fix => vec!["FIX".into()],
        }
    }
}
