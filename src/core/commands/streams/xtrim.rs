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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_xtrim_maxlen_exact() {
        let c = XTrim::parse(&[bs("k"), bs("MAXLEN"), bs("100")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert!(matches!(
            c.strategy,
            TrimStrategy::MaxLen {
                approx: false,
                count: 100
            }
        ));
        assert!(c.limit.is_none());
    }

    #[test]
    fn test_xtrim_maxlen_approximate() {
        let c = XTrim::parse(&[bs("k"), bs("MAXLEN"), bs("~"), bs("100")]).unwrap();
        assert!(matches!(
            c.strategy,
            TrimStrategy::MaxLen {
                approx: true,
                count: 100
            }
        ));
    }

    #[test]
    fn test_xtrim_minid_exact() {
        let c = XTrim::parse(&[bs("k"), bs("MINID"), bs("1-0")]).unwrap();
        assert!(matches!(
            c.strategy,
            TrimStrategy::MinId { approx: false, threshold } if threshold == StreamId::new(1, 0)
        ));
    }

    #[test]
    fn test_xtrim_minid_approximate() {
        let c = XTrim::parse(&[bs("k"), bs("MINID"), bs("~"), bs("1-0")]).unwrap();
        assert!(matches!(
            c.strategy,
            TrimStrategy::MinId { approx: true, .. }
        ));
    }

    #[test]
    fn test_xtrim_with_limit() {
        let c = XTrim::parse(&[bs("k"), bs("LIMIT"), bs("5"), bs("MAXLEN"), bs("100")]).unwrap();
        assert_eq!(c.limit, Some(5));
    }

    #[test]
    fn test_xtrim_strategy_case_insensitive() {
        let c = XTrim::parse(&[bs("k"), bs("maxlen"), bs("100")]).unwrap();
        assert!(matches!(
            c.strategy,
            TrimStrategy::MaxLen { count: 100, .. }
        ));
    }

    #[test]
    fn test_xtrim_with_too_few_args_is_error() {
        let r = XTrim::parse(&[bs("k"), bs("MAXLEN")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_xtrim_with_no_args_is_error() {
        let r = XTrim::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_xtrim_with_unknown_strategy_is_syntax_error() {
        let r = XTrim::parse(&[bs("k"), bs("FOO"), bs("100")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_xtrim_with_non_bulk_key_is_wrong_type() {
        let r = XTrim::parse(&[RespFrame::Integer(1), bs("MAXLEN"), bs("100")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_xtrim_to_resp_args_round_trips_maxlen() {
        let c = XTrim::parse(&[bs("k"), bs("MAXLEN"), bs("~"), bs("50")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[1], Bytes::from_static(b"MAXLEN"));
        assert_eq!(args[2], Bytes::from_static(b"~"));
        assert_eq!(args[3], Bytes::from_static(b"50"));
    }

    #[test]
    fn test_xtrim_to_resp_args_round_trips_minid_with_limit() {
        let c = XTrim::parse(&[bs("k"), bs("LIMIT"), bs("3"), bs("MINID"), bs("1-0")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[1], Bytes::from_static(b"LIMIT"));
        assert_eq!(args[3], Bytes::from_static(b"MINID"));
    }
}
