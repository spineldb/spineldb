// src/core/commands/streams/xtrim.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::stream::StreamId;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone)]
pub enum TrimStrategy {
    MaxLen { approx: bool, count: usize },
    MinId { approx: bool, threshold: StreamId },
}

#[derive(Debug, Clone)]
pub struct XTrim {
    pub key: Bytes,
    pub strategy: TrimStrategy,
    pub limit: Option<usize>,
}

impl Default for XTrim {
    fn default() -> Self {
        Self {
            key: Default::default(),
            strategy: TrimStrategy::MaxLen {
                approx: false,
                count: 0,
            },
            limit: None,
        }
    }
}

impl ParseCommand for XTrim {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("XTRIM".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let mut i = 1;
        let mut limit = None;

        if args.len() > i + 1 && extract_string(&args[i])?.eq_ignore_ascii_case("LIMIT") {
            i += 1;
            limit = Some(extract_string(&args[i])?.parse()?);
            i += 1;
        }

        let strategy_name = extract_string(&args[i])?.to_ascii_lowercase();
        i += 1;

        let strategy = match strategy_name.as_str() {
            "maxlen" => {
                let approx = if args
                    .get(i)
                    .is_some_and(|f| extract_string(f).unwrap_or_default() == "~")
                {
                    i += 1;
                    true
                } else {
                    false
                };
                if i >= args.len() {
                    return Err(SpinelDBError::SyntaxError);
                }
                let count = extract_string(&args[i])?.parse()?;
                TrimStrategy::MaxLen { approx, count }
            }
            "minid" => {
                let approx = if args
                    .get(i)
                    .is_some_and(|f| extract_string(f).unwrap_or_default() == "~")
                {
                    i += 1;
                    true
                } else {
                    false
                };
                if i >= args.len() {
                    return Err(SpinelDBError::SyntaxError);
                }
                let threshold = extract_string(&args[i])?
                    .parse::<StreamId>()
                    .map_err(|e| SpinelDBError::InvalidState(e.to_string()))?;
                TrimStrategy::MinId { approx, threshold }
            }
            _ => return Err(SpinelDBError::SyntaxError),
        };
        Ok(XTrim {
            key,
            strategy,
            limit,
        })
    }
}

#[async_trait]
impl ExecutableCommand for XTrim {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (shard, guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = guard.get_mut(&self.key) else {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            guard.pop(&self.key);
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        if let DataValue::Stream(stream) = &mut entry.data {
            let old_mem = stream.memory_usage();

            let to_remove: Vec<StreamId> = match &self.strategy {
                TrimStrategy::MaxLen { count, .. } => {
                    if stream.length as usize <= *count {
                        vec![] // Tidak ada yang perlu dihapus
                    } else {
                        let num_to_remove =
                            (stream.length as usize - *count).min(self.limit.unwrap_or(usize::MAX));
                        stream.entries.keys().take(num_to_remove).cloned().collect()
                    }
                }
                TrimStrategy::MinId { threshold, .. } => stream
                    .entries
                    .keys()
                    .take_while(|&id| id < threshold)
                    .take(self.limit.unwrap_or(usize::MAX))
                    .cloned()
                    .collect(),
            };

            let removed_count = to_remove.len();
            if removed_count > 0 {
                for id in to_remove {
                    if stream.entries.remove(&id).is_some() {
                        stream.length -= 1;
                    }
                }

                // Update metadata setelah semua operasi selesai
                let new_mem = stream.memory_usage();
                entry.size = new_mem;
                if old_mem > new_mem {
                    shard
                        .current_memory
                        .fetch_sub(old_mem - new_mem, Ordering::Relaxed);
                }
                entry.version += 1;

                Ok((
                    RespValue::Integer(removed_count as i64),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for XTrim {
    fn name(&self) -> &'static str {
        "xtrim"
    }
    fn arity(&self) -> i64 {
        -3
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
        if let Some(l) = self.limit {
            args.extend([Bytes::from_static(b"LIMIT"), l.to_string().into()]);
        }
        match &self.strategy {
            TrimStrategy::MaxLen { approx, count } => {
                args.push("MAXLEN".into());
                if *approx {
                    args.push("~".into());
                }
                args.push(count.to_string().into());
            }
            TrimStrategy::MinId { approx, threshold } => {
                args.push("MINID".into());
                if *approx {
                    args.push("~".into());
                }
                args.push(threshold.to_string().into());
            }
        }
        args
    }
}
