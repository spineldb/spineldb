// src/core/commands/zset/zset_ops_logic.rs

use crate::core::SpinelDBError;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::storage::db::zset::SortedSet;
use bytes::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Aggregate {
    #[default]
    Sum,
    Min,
    Max,
}

/// Helper to get a clone of a SortedSet from a given key, handling expiration and type checking.
pub(super) async fn get_zset_from_guard(
    key: &Bytes,
    ctx: &mut ExecutionContext<'_>,
) -> Result<Option<SortedSet>, SpinelDBError> {
    use crate::core::storage::db::ExecutionLocks;
    let guards = match &mut ctx.locks {
        ExecutionLocks::Multi { guards } => guards,
        _ => {
            return Err(SpinelDBError::Internal(
                "ZSet op requires multi-key lock".into(),
            ));
        }
    };
    let shard_index = ctx.db.get_shard_index(key);
    let guard = guards
        .get_mut(&shard_index)
        .ok_or_else(|| SpinelDBError::Internal("Missing shard lock for zset operation".into()))?;

    if let Some(entry) = guard.get_mut(key) {
        if entry.is_expired() {
            guard.pop(key);
            return Ok(None);
        }
        match &entry.data {
            DataValue::SortedSet(zset) => Ok(Some(zset.clone())),
            _ => Err(SpinelDBError::WrongType),
        }
    } else {
        Ok(None)
    }
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
    ) -> Result<(), SpinelDBError> {
        use crate::core::storage::db::ExecutionLocks;
        let guards = match &mut ctx.locks {
            ExecutionLocks::Multi { guards } => guards,
            _ => {
                return Err(SpinelDBError::Internal(
                    "ZSet store op requires multi-key lock".into(),
                ));
            }
        };
        let shard_index = ctx.db.get_shard_index(&dest_key);
        let guard = guards
            .get_mut(&shard_index)
            .ok_or_else(|| SpinelDBError::Internal("Missing dest lock".into()))?;

        if zset.is_empty() {
            guard.pop(&dest_key);
        } else {
            let new_value = StoredValue::new(DataValue::SortedSet(zset));
            guard.put(dest_key, new_value);
        }
        Ok(())
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
