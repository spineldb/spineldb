// src/core/commands/set/spop.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use rand::seq::IteratorRandom;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct SPop {
    pub key: Bytes,
    pub count: Option<usize>,
}
impl ParseCommand for SPop {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 2 {
            return Err(SpinelDBError::WrongArgumentCount("SPOP".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let count = if args.len() == 2 {
            Some(
                extract_string(&args[1])?
                    .parse::<usize>()
                    .map_err(|_| SpinelDBError::NotAnInteger)?,
            )
        } else {
            None
        };
        Ok(SPop { key, count })
    }
}

#[async_trait]
impl ExecutableCommand for SPop {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            return Ok((
                if self.count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                },
                WriteOutcome::DidNotWrite,
            ));
        };
        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((
                if self.count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                },
                WriteOutcome::DidNotWrite,
            ));
        }

        if let DataValue::Set(set) = &mut entry.data {
            if set.is_empty() {
                return Ok((
                    if self.count.is_some() {
                        RespValue::Array(vec![])
                    } else {
                        RespValue::Null
                    },
                    WriteOutcome::DidNotWrite,
                ));
            }

            let mut rng = rand::thread_rng();
            let count = self.count.unwrap_or(1);
            let members_to_pop: Vec<Bytes> = set.iter().cloned().choose_multiple(&mut rng, count);

            if members_to_pop.is_empty() {
                return Ok((
                    if self.count.is_some() {
                        RespValue::Array(vec![])
                    } else {
                        RespValue::Null
                    },
                    WriteOutcome::DidNotWrite,
                ));
            }

            let mut mem_freed = 0;
            for member in &members_to_pop {
                if set.remove(member) {
                    mem_freed += member.len();
                }
            }

            entry.size -= mem_freed;
            entry.version = entry.version.wrapping_add(1);
            shard.current_memory.fetch_sub(mem_freed, Ordering::Relaxed);

            let outcome = if set.is_empty() {
                shard_cache_guard.pop(&self.key);
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::Write { keys_modified: 1 }
            };

            let response = if self.count.is_some() {
                RespValue::Array(
                    members_to_pop
                        .into_iter()
                        .map(RespValue::BulkString)
                        .collect(),
                )
            } else {
                RespValue::BulkString(members_to_pop.into_iter().next().unwrap())
            };

            Ok((response, outcome))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for SPop {
    fn name(&self) -> &'static str {
        "spop"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.key.clone()];
        if let Some(c) = self.count {
            args.push(c.to_string().into());
        }
        args
    }
}
