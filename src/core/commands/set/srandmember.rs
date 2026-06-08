// src/core/commands/set/srandmember.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use rand::seq::IteratorRandom;

#[derive(Debug, Clone, Default)]
pub struct SrandMember {
    pub key: Bytes,
    pub count: Option<i64>,
}

impl ParseCommand for SrandMember {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 2 {
            return Err(SpinelDBError::WrongArgumentCount("SRANDMEMBER".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let count = if args.len() == 2 {
            Some(
                extract_string(&args[1])?
                    .parse::<i64>()
                    .map_err(|_| SpinelDBError::NotAnInteger)?,
            )
        } else {
            None
        };
        Ok(SrandMember { key, count })
    }
}

#[async_trait]
impl ExecutableCommand for SrandMember {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let resp = if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                if self.count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                }
            } else if let DataValue::Set(set) = &entry.data {
                if set.is_empty() {
                    if self.count.is_some() {
                        RespValue::Array(vec![])
                    } else {
                        RespValue::Null
                    }
                } else {
                    let mut rng = rand::thread_rng();
                    match self.count {
                        None => {
                            let member = set.iter().choose(&mut rng).unwrap();
                            RespValue::BulkString(member.clone())
                        }
                        Some(count) => {
                            let mut results = vec![];
                            if count > 0 {
                                // Ambil `count` member unik
                                for member in set
                                    .iter()
                                    .cloned()
                                    .choose_multiple(&mut rng, count as usize)
                                {
                                    results.push(RespValue::BulkString(member));
                                }
                            } else {
                                // Ambil `abs(count)` member, boleh duplikat
                                for _ in 0..count.abs() {
                                    let member = set.iter().choose(&mut rng).unwrap();
                                    results.push(RespValue::BulkString(member.clone()));
                                }
                            }
                            RespValue::Array(results)
                        }
                    }
                }
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else if self.count.is_some() {
            RespValue::Array(vec![])
        } else {
            RespValue::Null
        };
        Ok((resp, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for SrandMember {
    fn name(&self) -> &'static str {
        "srandmember"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_srandmember_parses_key_only() {
        let c = SrandMember::parse(&[bs("k")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.count, None);
    }

    #[test]
    fn test_srandmember_parses_with_positive_count() {
        let c = SrandMember::parse(&[bs("k"), bs("3")]).unwrap();
        assert_eq!(c.count, Some(3));
    }

    #[test]
    fn test_srandmember_parses_with_negative_count() {
        let c = SrandMember::parse(&[bs("k"), bs("-2")]).unwrap();
        assert_eq!(c.count, Some(-2));
    }

    #[test]
    fn test_srandmember_with_no_args_is_error() {
        let r = SrandMember::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_srandmember_with_too_many_args_is_error() {
        let r = SrandMember::parse(&[bs("k"), bs("3"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_srandmember_with_non_integer_count_is_error() {
        let r = SrandMember::parse(&[bs("k"), bs("all")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_srandmember_to_resp_args_round_trips() {
        let c = SrandMember::parse(&[bs("k"), bs("5")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(
            args,
            vec![Bytes::from_static(b"k"), Bytes::from_static(b"5")]
        );
    }
}
