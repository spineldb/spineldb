// src/core/commands/list/lpos.rs

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

/// Represents the `LPOS` command with all its parsed options.
#[derive(Debug, Clone, Default)]
pub struct LPos {
    pub key: Bytes,
    pub element: Bytes,
    pub rank: Option<i64>,
    pub count: Option<u64>,
    pub max_len: Option<u64>,
}

impl ParseCommand for LPos {
    /// Parses the `LPOS` command arguments from a RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("LPOS".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let element = extract_bytes(&args[1])?;

        let mut rank = None;
        let mut count = None;
        let mut max_len = None;

        let mut i = 2;
        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "rank" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    rank = Some(
                        extract_string(&args[i])?
                            .parse()
                            .map_err(|_| SpinelDBError::NotAnInteger)?,
                    );
                }
                "count" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let c: u64 = extract_string(&args[i])?
                        .parse()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;

                    // COUNT 0 means find all occurrences.
                    // Represent this internally as u64::MAX to simplify the loop condition.
                    count = if c == 0 { Some(u64::MAX) } else { Some(c) };
                }
                "maxlen" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    max_len = Some(
                        extract_string(&args[i])?
                            .parse()
                            .map_err(|_| SpinelDBError::NotAnInteger)?,
                    );
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
            i += 1;
        }

        Ok(LPos {
            key,
            element,
            rank,
            count,
            max_len,
        })
    }
}

#[async_trait]
impl ExecutableCommand for LPos {
    /// Executes the `LPOS` command.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // First, check if the key exists and is a list.
        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            // If the key does not exist, return Null or an empty array depending on COUNT.
            return Ok((
                if self.count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                },
                WriteOutcome::DidNotWrite,
            ));
        };

        // Perform passive deletion if the key is expired.
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

        let DataValue::List(list) = &entry.data else {
            // If the key exists but is not a list.
            return Err(SpinelDBError::WrongType);
        };

        let rank = self.rank.unwrap_or(1);
        let mut occurrences_found = 0i64;
        let mut positions = Vec::new();

        // Determine the iteration direction based on the RANK (positive or negative).
        let iter: Box<dyn Iterator<Item = (usize, &Bytes)>> = if rank > 0 {
            Box::new(list.iter().enumerate())
        } else {
            Box::new(list.iter().enumerate().rev())
        };

        let mut comparisons = 0u64;
        for (i, v) in iter {
            // Check the MAXLEN limit if it is specified.
            if let Some(ml) = self.max_len {
                if ml > 0 && comparisons >= ml {
                    break;
                }
                comparisons += 1;
            }

            if v == &self.element {
                occurrences_found += 1;

                // Check if this occurrence matches the requested RANK.
                // rank.abs() is used because a negative rank only changes search direction.
                if occurrences_found >= rank.abs() {
                    if self.count.is_some() {
                        // If COUNT is present, collect the positions.
                        positions.push(RespValue::Integer(i as i64));

                        // Stop if the number of requested occurrences is reached.
                        // `u64::MAX` (our internal representation for COUNT 0) will never be reached.
                        if let Some(c) = self.count
                            && positions.len() as u64 >= c
                        {
                            break;
                        }
                    } else {
                        // If COUNT is not present, we only need the first matching rank.
                        // Return its position immediately.
                        return Ok((RespValue::Integer(i as i64), WriteOutcome::DidNotWrite));
                    }
                }
            }
        }

        // After the loop, return the result based on whether COUNT was used.
        if self.count.is_some() {
            Ok((RespValue::Array(positions), WriteOutcome::DidNotWrite))
        } else {
            // Loop finished, but a matching rank was not found (when without COUNT).
            Ok((RespValue::Null, WriteOutcome::DidNotWrite))
        }
    }
}

impl CommandSpec for LPos {
    fn name(&self) -> &'static str {
        "lpos"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone(), self.element.clone()];
        if let Some(r) = self.rank {
            args.extend([Bytes::from_static(b"RANK"), r.to_string().into()]);
        }
        if let Some(c) = self.count {
            // Convert u64::MAX (our internal representation for COUNT 0)
            // back to the string "0" for accurate AOF/Replication.
            let count_str = if c == u64::MAX {
                "0".to_string()
            } else {
                c.to_string()
            };
            args.extend([Bytes::from_static(b"COUNT"), count_str.into()]);
        }
        if let Some(m) = self.max_len {
            args.extend([Bytes::from_static(b"MAXLEN"), m.to_string().into()]);
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
    fn test_lpos_parses_key_and_element() {
        let c = LPos::parse(&[bs("k"), bs("v")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.element, Bytes::from_static(b"v"));
        assert_eq!(c.rank, None);
        assert_eq!(c.count, None);
        assert_eq!(c.max_len, None);
    }

    #[test]
    fn test_lpos_parses_rank() {
        let c = LPos::parse(&[bs("k"), bs("v"), bs("RANK"), bs("2")]).unwrap();
        assert_eq!(c.rank, Some(2));
    }

    #[test]
    fn test_lpos_parses_count() {
        let c = LPos::parse(&[bs("k"), bs("v"), bs("COUNT"), bs("3")]).unwrap();
        assert_eq!(c.count, Some(3));
    }

    #[test]
    fn test_lpos_parses_count_zero_as_max() {
        let c = LPos::parse(&[bs("k"), bs("v"), bs("COUNT"), bs("0")]).unwrap();
        assert_eq!(c.count, Some(u64::MAX));
    }

    #[test]
    fn test_lpos_parses_maxlen() {
        let c = LPos::parse(&[bs("k"), bs("v"), bs("MAXLEN"), bs("100")]).unwrap();
        assert_eq!(c.max_len, Some(100));
    }

    #[test]
    fn test_lpos_parses_all_options() {
        let c = LPos::parse(&[
            bs("k"),
            bs("v"),
            bs("RANK"),
            bs("1"),
            bs("COUNT"),
            bs("2"),
            bs("MAXLEN"),
            bs("10"),
        ])
        .unwrap();
        assert_eq!(c.rank, Some(1));
        assert_eq!(c.count, Some(2));
        assert_eq!(c.max_len, Some(10));
    }

    #[test]
    fn test_lpos_options_case_insensitive() {
        let c = LPos::parse(&[bs("k"), bs("v"), bs("rank"), bs("1")]).unwrap();
        assert_eq!(c.rank, Some(1));
    }

    #[test]
    fn test_lpos_with_no_args_is_error() {
        let r = LPos::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lpos_with_too_few_args_is_error() {
        let r = LPos::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lpos_with_unknown_option_is_syntax_error() {
        let r = LPos::parse(&[bs("k"), bs("v"), bs("FOO"), bs("1")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_lpos_with_option_missing_value_is_syntax_error() {
        let r = LPos::parse(&[bs("k"), bs("v"), bs("RANK")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_lpos_with_non_integer_rank_is_error() {
        let r = LPos::parse(&[bs("k"), bs("v"), bs("RANK"), bs("first")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_lpos_to_resp_args_with_count_zero_round_trips() {
        let c = LPos::parse(&[bs("k"), bs("v"), bs("COUNT"), bs("0")]).unwrap();
        let args = c.to_resp_args();
        // [k, v, COUNT, 0]
        assert_eq!(args.len(), 4);
        assert_eq!(args[2], Bytes::from_static(b"COUNT"));
        assert_eq!(args[3], Bytes::from_static(b"0"));
    }
}
