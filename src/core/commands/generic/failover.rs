// src/core/commands/generic/failover.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

#[derive(Debug, Clone)]
pub enum FailoverSubcommand {
    Poison { run_id: String, ttl_secs: u64 },
}

#[derive(Debug, Clone, Default)]
pub struct Failover {
    pub subcommand: FailoverSubcommand,
}

impl Default for FailoverSubcommand {
    fn default() -> Self {
        FailoverSubcommand::Poison {
            run_id: String::new(),
            ttl_secs: 0,
        }
    }
}

impl ParseCommand for Failover {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("FAILOVER".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "poison" => {
                validate_arg_count(&args[1..], 2, "FAILOVER POISON")?;
                let run_id = extract_string(&args[1])?;
                let ttl_secs = extract_string(&args[2])?.parse()?;
                FailoverSubcommand::Poison { run_id, ttl_secs }
            }
            _ => return Err(SpinelDBError::UnknownCommand("FAILOVER".to_string())),
        };

        Ok(Failover { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Failover {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            FailoverSubcommand::Poison { run_id, ttl_secs } => {
                // Calculate the absolute expiry time as a UNIX timestamp in seconds.
                // This is robust against system clock changes during server downtime.
                let now_unix_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let expiry_timestamp = now_unix_secs + ttl_secs;

                // Use the entry API to ensure the poison TTL is only ever extended, not shortened.
                // This prevents a late or misconfigured command from reducing the safety window.
                ctx.state
                    .replication
                    .poisoned_masters
                    .entry(run_id.clone())
                    .and_modify(|current_expiry| {
                        // If an entry exists, only update it if the new expiry is later.
                        if expiry_timestamp > *current_expiry {
                            *current_expiry = expiry_timestamp;
                        }
                    })
                    // If the entry does not exist, insert it with the new expiry.
                    .or_insert(expiry_timestamp);

                // Persist the new state to disk.
                if let Err(e) = ctx.state.replication.save_poisoned_masters_to_disk() {
                    // Log a warning but don't fail the command, as the in-memory
                    // state is still updated. The state will be lost on restart if this fails.
                    warn!("Failed to persist poisoned masters state to disk: {}", e);
                }

                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
        }
    }
}

impl CommandSpec for Failover {
    fn name(&self) -> &'static str {
        "failover"
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
            FailoverSubcommand::Poison { run_id, ttl_secs } => vec![
                "POISON".into(),
                run_id.clone().into(),
                ttl_secs.to_string().into(),
            ],
        }
    }
}
