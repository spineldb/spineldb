// src/core/commands/zset/zrangestore.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::zset::SortedSet;
use crate::core::storage::db::{ExecutionContext, ExecutionLocks};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone)]
enum RangeType {
    Index,
    Score,
    Lex,
}

#[derive(Debug, Clone)]
pub struct ZRangeStore {
    destination: Bytes,
    source: Bytes,
    range_type: RangeType,
    min_str: String,
    max_str: String,
    rev: bool,
    limit: Option<(usize, usize)>,
}

impl Default for ZRangeStore {
    fn default() -> Self {
        Self {
            destination: Bytes::new(),
            source: Bytes::new(),
            range_type: RangeType::Index,
            min_str: "0".to_string(),
            max_str: "-1".to_string(),
            rev: false,
            limit: None,
        }
    }
}

impl ParseCommand for ZRangeStore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 4 {
            return Err(SpinelDBError::WrongArgumentCount("ZRANGESTORE".to_string()));
        }
        let destination = extract_bytes(&args[0])?;
        let source = extract_bytes(&args[1])?;
        let min_str = extract_string(&args[2])?;
        let max_str = extract_string(&args[3])?;

        let mut range_type = RangeType::Index;
        let mut rev = false;
        let mut limit = None;
        let mut i = 4;
        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "bylex" => range_type = RangeType::Lex,
                "byscore" => range_type = RangeType::Score,
                "rev" => rev = true,
                "limit" => {
                    i += 1;
                    if i + 1 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let offset = extract_string(&args[i])?.parse()?;
                    i += 1;
                    let count = extract_string(&args[i])?.parse()?;
                    limit = Some((offset, count));
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
            i += 1;
        }

        Ok(ZRangeStore {
            destination,
            source,
            range_type,
            min_str,
            max_str,
            rev,
            limit,
        })
    }
}

#[async_trait]
impl ExecutableCommand for ZRangeStore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "ZRANGESTORE requires multi-key lock".into(),
                ));
            }
        };

        let source_shard_index = ctx.db.get_shard_index(&self.source);
        let source_guard = guards
            .get_mut(&source_shard_index)
            .ok_or_else(|| SpinelDBError::Internal("Missing source lock".into()))?;

        let source_zset = if let Some(entry) = source_guard.get_mut(&self.source) {
            if entry.is_expired() {
                None
            } else if let DataValue::SortedSet(zset) = &entry.data {
                Some(zset.clone())
            } else {
                return Err(SpinelDBError::WrongType);
            }
        } else {
            None
        };

        let result_entries = if let Some(zset) = source_zset {
            let mut entries = match self.range_type {
                RangeType::Index => {
                    let start: i64 = self.min_str.parse()?;
                    let stop: i64 = self.max_str.parse()?;
                    if self.rev {
                        zset.get_rev_range(start, stop)
                    } else {
                        zset.get_range(start, stop)
                    }
                }
                RangeType::Score => {
                    let min =
                        crate::core::commands::zset::helpers::parse_score_boundary(&self.min_str)?;
                    let max =
                        crate::core::commands::zset::helpers::parse_score_boundary(&self.max_str)?;
                    let mut entries = zset.get_range_by_score(min, max);
                    if self.rev {
                        entries.reverse();
                    }
                    entries
                }
                RangeType::Lex => {
                    if !zset.scores_are_all_equal() {
                        return Err(SpinelDBError::WrongType);
                    }
                    let min =
                        crate::core::commands::zset::helpers::parse_lex_boundary(&self.min_str)?;
                    let max =
                        crate::core::commands::zset::helpers::parse_lex_boundary(&self.max_str)?;
                    let mut entries = zset.get_range_by_lex(&min, &max);
                    if self.rev {
                        entries.reverse();
                    }
                    entries
                }
            };
            if let Some((offset, count)) = self.limit {
                if count > 0 && offset < entries.len() {
                    entries = entries.into_iter().skip(offset).take(count).collect();
                } else {
                    // COUNT 0 atau offset di luar jangkauan berarti 0 hasil
                    entries.clear();
                }
            }
            entries
        } else {
            vec![]
        };

        let stored_len = result_entries.len();
        let mut new_zset = SortedSet::new();
        for entry in result_entries {
            new_zset.add(entry.score, entry.member);
        }

        let dest_shard_index = ctx.db.get_shard_index(&self.destination);
        let dest_guard = guards
            .get_mut(&dest_shard_index)
            .ok_or_else(|| SpinelDBError::Internal("Missing destination lock".into()))?;

        if stored_len == 0 {
            dest_guard.pop(&self.destination);
        } else {
            let new_value = StoredValue::new(DataValue::SortedSet(new_zset));
            dest_guard.put(self.destination.clone(), new_value);
        }

        Ok((
            RespValue::Integer(stored_len as i64),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for ZRangeStore {
    fn name(&self) -> &'static str {
        "zrangestore"
    }
    fn arity(&self) -> i64 {
        -4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.destination.clone(), self.source.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![
            self.destination.clone(),
            self.source.clone(),
            self.min_str.clone().into(),
            self.max_str.clone().into(),
        ];

        match self.range_type {
            RangeType::Score => args.push(Bytes::from_static(b"BYSCORE")),
            RangeType::Lex => args.push(Bytes::from_static(b"BYLEX")),
            RangeType::Index => {}
        }
        if self.rev {
            args.push(Bytes::from_static(b"REV"));
        }
        if let Some((offset, count)) = self.limit {
            args.push(Bytes::from_static(b"LIMIT"));
            args.push(offset.to_string().into());
            args.push(count.to_string().into());
        }
        args
    }
}
