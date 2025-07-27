// src/core/commands/zset/zset_ops_logic.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::commands::helpers::extract_string;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::zset::SortedSet;
use crate::core::storage::db::{Db, ExecutionContext, ExecutionLocks, ShardCache};
use crate::core::{RespValue, SpinelDBError};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::MutexGuard;

/// Defines the aggregation function for ZUNIONSTORE and ZINTERSTORE.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Aggregate {
    #[default]
    Sum,
    Min,
    Max,
}

/// Helper to get a clone of a SortedSet from a given key.
/// Returns WRONGTYPE error if the key exists but is not a Set.
/// Returns Ok(None) if the key does not exist or is expired.
pub(super) fn get_zset_from_guard<'a>(
    key: &Bytes,
    db: &Db,
    guards: &mut BTreeMap<usize, MutexGuard<'a, ShardCache>>,
) -> Result<Option<SortedSet>, SpinelDBError> {
    let shard_index = db.get_shard_index(key);
    let guard = guards
        .get_mut(&shard_index)
        .ok_or_else(|| SpinelDBError::Internal("Missing shard lock for zset operation".into()))?;

    if let Some(entry) = guard.get_mut(key) {
        if entry.is_expired() {
            guard.pop(key);
            Ok(None)
        } else {
            match &entry.data {
                DataValue::SortedSet(zset) => Ok(Some(zset.clone())),
                _ => Err(SpinelDBError::WrongType),
            }
        }
    } else {
        Ok(None)
    }
}

/// Helper to parse the shared `[WEIGHTS ...]` and `[AGGREGATE ...]` options.
pub(super) fn parse_store_args(
    args: &[RespFrame],
    num_keys: usize,
) -> Result<(Vec<f64>, Aggregate), SpinelDBError> {
    let mut weights = vec![1.0; num_keys];
    let mut aggregate = Aggregate::Sum;
    let mut i = 0;

    while i < args.len() {
        let option = extract_string(&args[i])?.to_ascii_lowercase();
        match option.as_str() {
            "weights" => {
                i += 1;
                if args.len() < i + num_keys {
                    return Err(SpinelDBError::SyntaxError);
                }
                weights = args[i..i + num_keys]
                    .iter()
                    .map(|f| {
                        extract_string(f)
                            .and_then(|s| s.parse::<f64>().map_err(|_| SpinelDBError::NotAFloat))
                    })
                    .collect::<Result<_, _>>()?;
                i += num_keys;
            }
            "aggregate" => {
                i += 1;
                if i >= args.len() {
                    return Err(SpinelDBError::SyntaxError);
                }
                aggregate = match extract_string(&args[i])?.to_ascii_lowercase().as_str() {
                    "sum" => Aggregate::Sum,
                    "min" => Aggregate::Min,
                    "max" => Aggregate::Max,
                    _ => return Err(SpinelDBError::SyntaxError),
                };
                i += 1;
            }
            _ => return Err(SpinelDBError::SyntaxError),
        }
    }
    Ok((weights, aggregate))
}

pub(super) struct ZSetOp;

impl ZSetOp {
    /// Performs a union operation on a slice of sorted sets.
    pub fn union(zsets: &[SortedSet], weights: &[f64], aggregate: Aggregate) -> SortedSet {
        let mut union_scores: HashMap<Bytes, f64> = HashMap::new();

        for (i, zset) in zsets.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            for entry in zset.iter() {
                let weighted_score = entry.score * weight;
                union_scores
                    .entry(entry.member.clone())
                    .and_modify(|s| *s = Self::apply_aggregate(*s, weighted_score, aggregate))
                    .or_insert(weighted_score);
            }
        }

        let mut result_zset = SortedSet::new();
        for (member, score) in union_scores {
            result_zset.add(score, member);
        }
        result_zset
    }

    /// Performs an efficient intersection on a slice of sorted sets.
    pub fn intersection(zsets: &[SortedSet], weights: &[f64], aggregate: Aggregate) -> SortedSet {
        if zsets.is_empty() {
            return SortedSet::new();
        }

        let smallest_set_idx = zsets
            .iter()
            .enumerate()
            .min_by_key(|(_, zset)| zset.len())
            .map(|(i, _)| i)
            .unwrap_or(0);

        let mut final_scores: HashMap<Bytes, f64> = HashMap::new();

        let other_sets: Vec<HashMap<Bytes, f64>> = zsets
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != smallest_set_idx)
            .map(|(_, zset)| zset.iter().map(|e| (e.member.clone(), e.score)).collect())
            .collect();

        let smallest_set = &zsets[smallest_set_idx];
        'member_loop: for base_entry in smallest_set.iter() {
            let base_member = &base_entry.member;
            let base_weight = weights.get(smallest_set_idx).copied().unwrap_or(1.0);
            let mut aggregated_score = base_entry.score * base_weight;

            let mut other_sets_idx = 0;

            for (i, _) in zsets.iter().enumerate() {
                if i == smallest_set_idx {
                    continue;
                }

                let other_set_map = &other_sets[other_sets_idx];
                other_sets_idx += 1;

                if let Some(other_score) = other_set_map.get(base_member) {
                    let other_weight = weights.get(i).copied().unwrap_or(1.0);
                    aggregated_score = Self::apply_aggregate(
                        aggregated_score,
                        other_score * other_weight,
                        aggregate,
                    );
                } else {
                    continue 'member_loop;
                }
            }

            final_scores.insert(base_member.clone(), aggregated_score);
        }

        let mut result_zset = SortedSet::new();
        for (member, score) in final_scores {
            result_zset.add(score, member);
        }
        result_zset
    }

    /// Stores the resulting sorted set into the destination key.
    pub fn store_result(
        dest_key: Bytes,
        zset: SortedSet,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "STORE op requires multi-key lock".into(),
                ));
            }
        };
        let dest_shard_index = ctx.db.get_shard_index(&dest_key);
        let dest_guard = guards
            .get_mut(&dest_shard_index)
            .ok_or_else(|| SpinelDBError::Internal("Missing dest lock for STORE".into()))?;

        let set_len = zset.len();

        if set_len == 0 {
            let existed = dest_guard.pop(&dest_key).is_some();
            let outcome = if existed {
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::DidNotWrite
            };
            return Ok((RespValue::Integer(0), outcome));
        }

        let new_value = StoredValue::new(DataValue::SortedSet(zset));

        dest_guard.put(dest_key, new_value);

        Ok((
            RespValue::Integer(set_len as i64),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }

    /// Helper to apply the aggregation function (SUM, MIN, MAX).
    fn apply_aggregate(s1: f64, s2: f64, aggregate: Aggregate) -> f64 {
        match aggregate {
            Aggregate::Sum => s1 + s2,
            Aggregate::Min => s1.min(s2),
            Aggregate::Max => s1.max(s2),
        }
    }
}
