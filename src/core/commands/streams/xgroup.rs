// src/core/commands/streams/xgroup.rs

//! Implements the `XGROUP` command and its various subcommands for managing
//! consumer groups within a stream.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::stream::{ConsumerGroup, StreamId};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;

/// Defines the subcommands for `XGROUP`. Each variant corresponds to a specific
/// action like creating or destroying a group.
#[derive(Debug, Clone)]
pub enum XGroupSubcommand {
    /// `XGROUP CREATE <key> <groupname> <id> [MKSTREAM]`
    Create {
        key: Bytes,
        group_name: Bytes,
        id: StreamId,
        mkstream: bool,
    },
    /// `XGROUP SETID <key> <groupname> <id>`
    SetId {
        key: Bytes,
        group_name: Bytes,
        id: StreamId,
    },
    /// `XGROUP DESTROY <key> <groupname>`
    Destroy { key: Bytes, group_name: Bytes },
    /// `XGROUP DELCONSUMER <key> <groupname> <consumername>`
    DelConsumer {
        key: Bytes,
        group_name: Bytes,
        consumer_name: Bytes,
    },
}

/// Represents the `XGROUP` command, dispatching to a specific subcommand.
#[derive(Debug, Clone)]
pub struct XGroup {
    pub subcommand: XGroupSubcommand,
}

impl Default for XGroup {
    /// Provides a default variant, required for command introspection.
    fn default() -> Self {
        Self {
            subcommand: XGroupSubcommand::Create {
                key: Default::default(),
                group_name: Default::default(),
                id: Default::default(),
                mkstream: false,
            },
        }
    }
}

impl XGroup {
    /// An internal constructor for `XGROUP CREATE`, used during AOF/SPLDB loading
    /// to deterministically reconstruct the state.
    pub fn new_create_internal(
        key: Bytes,
        group_name: Bytes,
        id: StreamId,
        mkstream: bool,
    ) -> Self {
        Self {
            subcommand: XGroupSubcommand::Create {
                key,
                group_name,
                id,
                mkstream,
            },
        }
    }
}

impl ParseCommand for XGroup {
    /// Parses the `XGROUP` command and its subcommand from a RESP frame array.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("XGROUP".to_string()));
        }

        // The first argument determines the subcommand to execute.
        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        match sub_str.as_str() {
            "create" => {
                if args.len() < 4 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "XGROUP CREATE".to_string(),
                    ));
                }
                let key = extract_bytes(&args[1])?;
                let group_name = extract_bytes(&args[2])?;
                let id_str = extract_string(&args[3])?;

                // Check for the optional MKSTREAM flag.
                let mut mkstream = false;
                if args.len() > 4 {
                    if args.len() == 5 && extract_string(&args[4])?.eq_ignore_ascii_case("mkstream")
                    {
                        mkstream = true;
                    } else {
                        return Err(SpinelDBError::SyntaxError);
                    }
                }

                // Handle the special '$' ID, which means the last entry in the stream.
                let id = if id_str == "$" {
                    // Use a sentinel value to be resolved during execution.
                    StreamId::new(u64::MAX, u64::MAX)
                } else {
                    id_str
                        .parse::<StreamId>()
                        .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?
                };

                Ok(XGroup {
                    subcommand: XGroupSubcommand::Create {
                        key,
                        group_name,
                        id,
                        mkstream,
                    },
                })
            }
            "setid" => {
                if args.len() != 4 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "XGROUP SETID".to_string(),
                    ));
                }
                let key = extract_bytes(&args[1])?;
                let group_name = extract_bytes(&args[2])?;
                let id_str = extract_string(&args[3])?;
                let id = id_str
                    .parse::<StreamId>()
                    .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?;
                Ok(XGroup {
                    subcommand: XGroupSubcommand::SetId {
                        key,
                        group_name,
                        id,
                    },
                })
            }
            "destroy" => {
                if args.len() != 3 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "XGROUP DESTROY".to_string(),
                    ));
                }
                let key = extract_bytes(&args[1])?;
                let group_name = extract_bytes(&args[2])?;
                Ok(XGroup {
                    subcommand: XGroupSubcommand::Destroy { key, group_name },
                })
            }
            "delconsumer" => {
                if args.len() != 4 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "XGROUP DELCONSUMER".to_string(),
                    ));
                }
                let key = extract_bytes(&args[1])?;
                let group_name = extract_bytes(&args[2])?;
                let consumer_name = extract_bytes(&args[3])?;
                Ok(XGroup {
                    subcommand: XGroupSubcommand::DelConsumer {
                        key,
                        group_name,
                        consumer_name,
                    },
                })
            }
            _ => Err(SpinelDBError::UnknownCommand(format!("XGROUP {sub_str}"))),
        }
    }
}

#[async_trait]
impl ExecutableCommand for XGroup {
    /// Executes the parsed `XGROUP` subcommand.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, guard) = ctx.get_single_shard_context_mut()?;

        match &self.subcommand {
            XGroupSubcommand::Create {
                key,
                group_name,
                id,
                mkstream,
            } => {
                let entry_exists = guard.peek(key).is_some_and(|e| !e.is_expired());

                // If MKSTREAM is not specified and the stream doesn't exist, return an error.
                if !entry_exists && !mkstream {
                    return Err(SpinelDBError::KeyNotFound);
                }

                // Get the stream, or create a new one if MKSTREAM is specified.
                let entry = guard.get_or_insert_with_mut(key.clone(), || {
                    crate::core::storage::data_types::StoredValue::new(DataValue::Stream(
                        Default::default(),
                    ))
                });

                if let DataValue::Stream(stream) = &mut entry.data {
                    // Check for BUSYGROUP error: the group must not already exist.
                    if stream.groups.contains_key(group_name) {
                        return Err(SpinelDBError::InvalidState(format!(
                            "-BUSYGROUP Consumer Group '{}' already exists",
                            String::from_utf8_lossy(group_name)
                        )));
                    }

                    // Resolve the '$' sentinel ID to the stream's last generated ID.
                    let start_id = if id.timestamp_ms == u64::MAX && id.sequence == u64::MAX {
                        stream.last_generated_id
                    } else {
                        *id
                    };

                    // Create the new consumer group state.
                    let new_group = ConsumerGroup {
                        name: group_name.clone(),
                        last_delivered_id: start_id,
                        consumers: Default::default(),
                        pending_entries: Default::default(),
                        idle_index: BTreeSet::new(), // Explicitly initialize the non-serialized index.
                    };

                    stream.groups.insert(group_name.clone(), new_group);
                    entry.version += 1; // Bump version for WATCH correctness.

                    Ok((
                        RespValue::SimpleString("OK".into()),
                        WriteOutcome::Write { keys_modified: 1 },
                    ))
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
            XGroupSubcommand::SetId {
                key,
                group_name,
                id,
            } => {
                let entry = guard.get_mut(key).ok_or(SpinelDBError::KeyNotFound)?;
                if entry.is_expired() {
                    return Err(SpinelDBError::KeyNotFound);
                }

                if let DataValue::Stream(stream) = &mut entry.data {
                    let group = stream
                        .groups
                        .get_mut(group_name)
                        .ok_or(SpinelDBError::ConsumerGroupNotFound)?;

                    group.last_delivered_id = *id;
                    entry.version += 1;
                    Ok((
                        RespValue::SimpleString("OK".into()),
                        WriteOutcome::Write { keys_modified: 1 },
                    ))
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
            XGroupSubcommand::Destroy { key, group_name } => {
                let entry = guard.get_mut(key).ok_or(SpinelDBError::KeyNotFound)?;
                if entry.is_expired() {
                    return Err(SpinelDBError::KeyNotFound);
                }

                if let DataValue::Stream(stream) = &mut entry.data {
                    if stream.groups.remove(group_name).is_some() {
                        entry.version += 1;
                        Ok((
                            RespValue::Integer(1),
                            WriteOutcome::Write { keys_modified: 1 },
                        ))
                    } else {
                        Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
                    }
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
            XGroupSubcommand::DelConsumer {
                key,
                group_name,
                consumer_name,
            } => {
                let entry = guard.get_mut(key).ok_or(SpinelDBError::KeyNotFound)?;
                if entry.is_expired() {
                    return Err(SpinelDBError::KeyNotFound);
                }

                if let DataValue::Stream(stream) = &mut entry.data {
                    let group = stream
                        .groups
                        .get_mut(group_name)
                        .ok_or(SpinelDBError::ConsumerGroupNotFound)?;

                    let mut removed_count = 0;
                    if group.consumers.remove(consumer_name).is_some() {
                        // Crucially, we must also remove all pending entries that
                        // belonged to the deleted consumer.
                        group.pending_entries.retain(|_, info| {
                            if info.consumer_name == *consumer_name {
                                removed_count += 1;
                                false // Remove this entry from the PEL.
                            } else {
                                true // Keep this entry.
                            }
                        });
                        entry.version += 1;
                    }
                    Ok((
                        RespValue::Integer(removed_count as i64),
                        WriteOutcome::Write { keys_modified: 1 },
                    ))
                } else {
                    Err(SpinelDBError::WrongType)
                }
            }
        }
    }
}

impl CommandSpec for XGroup {
    fn name(&self) -> &'static str {
        "xgroup"
    }

    fn arity(&self) -> i64 {
        // Arity is variable depending on the subcommand.
        match &self.subcommand {
            XGroupSubcommand::Create { .. } => -4,
            XGroupSubcommand::SetId { .. } => 4,
            XGroupSubcommand::Destroy { .. } => 3,
            XGroupSubcommand::DelConsumer { .. } => 4,
        }
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }

    fn first_key(&self) -> i64 {
        2 // The key is always the second argument after the subcommand name.
    }

    fn last_key(&self) -> i64 {
        2
    }

    fn step(&self) -> i64 {
        1
    }

    fn get_keys(&self) -> Vec<Bytes> {
        // Extract the stream key for cluster routing and ACLs.
        match &self.subcommand {
            XGroupSubcommand::Create { key, .. }
            | XGroupSubcommand::SetId { key, .. }
            | XGroupSubcommand::Destroy { key, .. }
            | XGroupSubcommand::DelConsumer { key, .. } => vec![key.clone()],
        }
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        // Reconstruct the command arguments for AOF/replication.
        match &self.subcommand {
            XGroupSubcommand::Create {
                key,
                group_name,
                id,
                mkstream,
            } => {
                let mut args = vec![
                    Bytes::from_static(b"CREATE"),
                    key.clone(),
                    group_name.clone(),
                ];
                // Convert the sentinel ID back to the '$' character for replication.
                let id_str = if id.timestamp_ms == u64::MAX && id.sequence == u64::MAX {
                    "$"
                } else {
                    &id.to_string()
                };
                args.push(Bytes::from(id_str.to_string()));
                if *mkstream {
                    args.push(Bytes::from_static(b"MKSTREAM"));
                }
                args
            }
            XGroupSubcommand::SetId {
                key,
                group_name,
                id,
            } => {
                vec![
                    Bytes::from_static(b"SETID"),
                    key.clone(),
                    group_name.clone(),
                    id.to_string().into(),
                ]
            }
            XGroupSubcommand::Destroy { key, group_name } => {
                vec![
                    Bytes::from_static(b"DESTROY"),
                    key.clone(),
                    group_name.clone(),
                ]
            }
            XGroupSubcommand::DelConsumer {
                key,
                group_name,
                consumer_name,
            } => {
                vec![
                    Bytes::from_static(b"DELCONSUMER"),
                    key.clone(),
                    group_name.clone(),
                    consumer_name.clone(),
                ]
            }
        }
    }
}
