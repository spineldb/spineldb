// src/core/database/zset.rs

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap, btree_set};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Bound;

/// Represents a single entry in a Sorted Set, containing a member and its score.
/// This struct implements `Ord`, `PartialEq`, etc., to allow it to be stored
/// in a `BTreeSet`, which keeps the entries sorted by score, then by member.
#[derive(Debug, Clone)]
pub struct ZSetEntry {
    pub score: f64,
    pub member: Bytes,
}

impl Ord for ZSetEntry {
    /// Defines the primary sorting order for entries: first by score, then lexicographically by member.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for ZSetEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ZSetEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.member == other.member
    }
}

impl Eq for ZSetEntry {}

impl Hash for ZSetEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Only the member is used for hashing, as it's the unique identifier within the set.
        self.member.hash(state);
    }
}

/// Defines a boundary for score-based range queries.
#[derive(Debug, Clone, PartialEq)]
pub enum ScoreBoundary {
    Inclusive(f64),
    Exclusive(f64),
    NegInfinity,
    PosInfinity,
}

impl fmt::Display for ScoreBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScoreBoundary::Inclusive(score) => write!(f, "{score}"),
            ScoreBoundary::Exclusive(score) => write!(f, "({score}"),
            ScoreBoundary::NegInfinity => write!(f, "-inf"),
            ScoreBoundary::PosInfinity => write!(f, "+inf"),
        }
    }
}

impl Default for ScoreBoundary {
    fn default() -> Self {
        ScoreBoundary::Inclusive(0.0)
    }
}

/// Defines a boundary for lexicographical range queries.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum LexBoundary {
    Inclusive(Bytes),
    Exclusive(Bytes),
    #[default]
    Min,
    Max,
}

impl fmt::Display for LexBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LexBoundary::Inclusive(b) => write!(f, "[{}", String::from_utf8_lossy(b)),
            LexBoundary::Exclusive(b) => write!(f, "({}", String::from_utf8_lossy(b)),
            LexBoundary::Min => write!(f, "-"),
            LexBoundary::Max => write!(f, "+"),
        }
    }
}

/// The main Sorted Set data structure.
/// It uses a `HashMap` for fast O(1) lookups of a member's score and a `BTreeSet`
/// to keep the entries sorted by score and member for efficient range queries.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SortedSet {
    /// Fast member-to-score lookups.
    members: HashMap<Bytes, f64>,
    /// Entries sorted by score, then member.
    sorted: BTreeSet<ZSetEntry>,
}

impl SortedSet {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn iter(&self) -> btree_set::Iter<'_, ZSetEntry> {
        self.sorted.iter()
    }

    /// Calculates the total memory usage of all members and scores in the set.
    pub fn memory_usage(&self) -> usize {
        let f64_size = std::mem::size_of::<f64>();
        // Use `.keys()` for clarity and efficiency as suggested by Clippy.
        self.members
            .keys()
            .map(|member| member.len() + f64_size)
            .sum()
    }

    /// Checks if all elements in the sorted set have the same score.
    /// This is a precondition for all lexicographical range commands.
    pub fn scores_are_all_equal(&self) -> bool {
        if self.len() < 2 {
            return true;
        }
        let first_score = self.sorted.iter().next().unwrap().score;
        self.sorted
            .iter()
            .all(|e| (e.score - first_score).abs() < f64::EPSILON)
    }

    /// Adds or updates a member in the sorted set.
    /// Returns `true` if a new element was added or an existing element's score was updated.
    pub fn add(&mut self, score: f64, member: Bytes) -> bool {
        if let Some(old_score) = self.members.insert(member.clone(), score) {
            if (old_score - score).abs() < f64::EPSILON {
                return false;
            }
            self.sorted.remove(&ZSetEntry {
                score: old_score,
                member: member.clone(),
            });
        }
        self.sorted.insert(ZSetEntry { score, member });
        true
    }

    /// Increments the score of a member. If the member does not exist, it's added with the increment as its score.
    pub fn increment_score(&mut self, member: &Bytes, increment: f64) -> f64 {
        let current_score = self.members.get(member).copied().unwrap_or(0.0);
        let new_score = current_score + increment;
        self.add(new_score, member.clone());
        new_score
    }

    /// Removes a member from the set. Returns `true` if the member was present.
    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.sorted.remove(&ZSetEntry {
                score,
                member: member.clone(),
            });
            true
        } else {
            false
        }
    }

    pub fn get_score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).copied()
    }

    pub fn contains_member(&self, member: &Bytes) -> bool {
        self.members.contains_key(member)
    }

    /// Returns the 0-based rank of a member, sorted from lowest to highest score.
    pub fn get_rank(&self, member: &Bytes) -> Option<usize> {
        self.members
            .get(member)
            .and_then(|_score| self.sorted.iter().position(|entry| entry.member == *member))
    }

    /// Returns the 0-based rank of a member, sorted from highest to lowest score.
    pub fn get_rev_rank(&self, member: &Bytes) -> Option<usize> {
        self.members.get(member).and_then(|_score| {
            self.sorted
                .iter()
                .rev()
                .position(|entry| entry.member == *member)
        })
    }

    /// Removes and returns the entry with the lowest score.
    pub fn pop_first(&mut self) -> Option<ZSetEntry> {
        if let Some(entry) = self.sorted.pop_first() {
            self.members.remove(&entry.member);
            Some(entry)
        } else {
            None
        }
    }

    /// Removes and returns the entry with the highest score.
    pub fn pop_last(&mut self) -> Option<ZSetEntry> {
        if let Some(entry) = self.sorted.pop_last() {
            self.members.remove(&entry.member);
            Some(entry)
        } else {
            None
        }
    }

    /// Returns a range of entries by rank (0-based index).
    pub fn get_range(&self, start: i64, stop: i64) -> Vec<ZSetEntry> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }
        let start = if start < 0 { len + start } else { start }.max(0);
        let stop = if stop < 0 { len + stop } else { stop }.min(len - 1);
        if start > stop || start >= len {
            return vec![];
        }
        self.sorted
            .iter()
            .skip(start as usize)
            .take((stop - start + 1) as usize)
            .cloned()
            .collect()
    }

    /// Returns a range of entries by rank, in reverse order.
    pub fn get_rev_range(&self, start: i64, stop: i64) -> Vec<ZSetEntry> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }
        let start = if start < 0 { len + start } else { start }.max(0);
        let stop = if stop < 0 { len + stop } else { stop }.min(len - 1);
        if start > stop || start >= len {
            return vec![];
        }
        self.sorted
            .iter()
            .rev()
            .skip(start as usize)
            .take((stop - start + 1) as usize)
            .cloned()
            .collect()
    }

    /// Returns a range of entries by score.
    pub fn get_range_by_score(&self, min: ScoreBoundary, max: ScoreBoundary) -> Vec<ZSetEntry> {
        let min_bound = match min {
            ScoreBoundary::Inclusive(score) => Bound::Included(ZSetEntry {
                score,
                member: Bytes::new(),
            }),
            ScoreBoundary::Exclusive(score) => Bound::Excluded(ZSetEntry {
                score,
                member: Bytes::from_static(&[255; 64]),
            }),
            ScoreBoundary::NegInfinity => Bound::Unbounded,
            ScoreBoundary::PosInfinity => return vec![],
        };

        let max_bound = match max {
            ScoreBoundary::Inclusive(score) => Bound::Included(ZSetEntry {
                score,
                member: Bytes::from_static(&[255; 64]),
            }),
            ScoreBoundary::Exclusive(score) => Bound::Excluded(ZSetEntry {
                score,
                member: Bytes::new(),
            }),
            ScoreBoundary::PosInfinity => Bound::Unbounded,
            ScoreBoundary::NegInfinity => return vec![],
        };

        self.sorted.range((min_bound, max_bound)).cloned().collect()
    }

    /// Removes entries within a score range.
    pub fn remove_range_by_score(&mut self, min: ScoreBoundary, max: ScoreBoundary) -> usize {
        let members_to_remove: Vec<Bytes> = {
            let min_bound = match min {
                ScoreBoundary::Inclusive(score) => Bound::Included(ZSetEntry {
                    score,
                    member: Bytes::new(),
                }),
                ScoreBoundary::Exclusive(score) => Bound::Excluded(ZSetEntry {
                    score,
                    member: Bytes::from_static(&[255; 64]),
                }),
                ScoreBoundary::NegInfinity => Bound::Unbounded,
                ScoreBoundary::PosInfinity => return 0,
            };

            let max_bound = match max {
                ScoreBoundary::Inclusive(score) => Bound::Included(ZSetEntry {
                    score,
                    member: Bytes::from_static(&[255; 64]),
                }),
                ScoreBoundary::Exclusive(score) => Bound::Excluded(ZSetEntry {
                    score,
                    member: Bytes::new(),
                }),
                ScoreBoundary::PosInfinity => Bound::Unbounded,
                ScoreBoundary::NegInfinity => return 0,
            };

            self.sorted
                .range((min_bound, max_bound))
                .map(|entry| entry.member.clone())
                .collect()
        };

        let count = members_to_remove.len();
        if count > 0 {
            for member in members_to_remove {
                self.remove(&member);
            }
        }
        count
    }

    /// Returns a range of entries by lexicographical order.
    pub fn get_range_by_lex(&self, min: &LexBoundary, max: &LexBoundary) -> Vec<ZSetEntry> {
        const LEX_SCORE: f64 = 0.0;

        let min_bound = match min {
            LexBoundary::Inclusive(b) => Bound::Included(ZSetEntry {
                score: LEX_SCORE,
                member: b.clone(),
            }),
            LexBoundary::Exclusive(b) => Bound::Excluded(ZSetEntry {
                score: LEX_SCORE,
                member: b.clone(),
            }),
            LexBoundary::Min => Bound::Unbounded,
            LexBoundary::Max => return vec![],
        };

        let max_bound = match max {
            LexBoundary::Inclusive(b) => Bound::Included(ZSetEntry {
                score: LEX_SCORE,
                member: b.clone(),
            }),
            LexBoundary::Exclusive(b) => Bound::Excluded(ZSetEntry {
                score: LEX_SCORE,
                member: b.clone(),
            }),
            LexBoundary::Max => Bound::Unbounded,
            LexBoundary::Min => return vec![],
        };

        self.sorted.range((min_bound, max_bound)).cloned().collect()
    }

    /// Removes entries within a lexicographical range.
    pub fn remove_range_by_lex(&mut self, min: &LexBoundary, max: &LexBoundary) -> usize {
        let members_to_remove: Vec<Bytes> = {
            const LEX_SCORE: f64 = 0.0;

            let min_bound = match min {
                LexBoundary::Inclusive(b) => Bound::Included(ZSetEntry {
                    score: LEX_SCORE,
                    member: b.clone(),
                }),
                LexBoundary::Exclusive(b) => Bound::Excluded(ZSetEntry {
                    score: LEX_SCORE,
                    member: b.clone(),
                }),
                LexBoundary::Min => Bound::Unbounded,
                LexBoundary::Max => return 0,
            };

            let max_bound = match max {
                LexBoundary::Inclusive(b) => Bound::Included(ZSetEntry {
                    score: LEX_SCORE,
                    member: b.clone(),
                }),
                LexBoundary::Exclusive(b) => Bound::Excluded(ZSetEntry {
                    score: LEX_SCORE,
                    member: b.clone(),
                }),
                LexBoundary::Max => Bound::Unbounded,
                LexBoundary::Min => return 0,
            };

            self.sorted
                .range((min_bound, max_bound))
                .map(|entry| entry.member.clone())
                .collect()
        };

        let count = members_to_remove.len();
        if count > 0 {
            for member in members_to_remove {
                self.remove(&member);
            }
        }
        count
    }

    /// Removes entries within a rank range.
    pub fn remove_range_by_rank(&mut self, start: i64, stop: i64) -> usize {
        let members_to_remove: Vec<Bytes> = {
            let len = self.len() as i64;
            if len == 0 {
                return 0;
            }
            let start = if start < 0 { len + start } else { start }.max(0);
            let stop = if stop < 0 { len + stop } else { stop }.min(len - 1);
            if start > stop || start >= len {
                return 0;
            }

            self.sorted
                .iter()
                .skip(start as usize)
                .take((stop - start + 1) as usize)
                .map(|entry| entry.member.clone())
                .collect()
        };

        let count = members_to_remove.len();
        if count > 0 {
            for member in members_to_remove {
                self.remove(&member);
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn make_zset() -> SortedSet {
        // Scores: a=1, b=2, c=3, d=4, e=5
        let mut zs = SortedSet::new();
        zs.add(1.0, b("a"));
        zs.add(2.0, b("b"));
        zs.add(3.0, b("c"));
        zs.add(4.0, b("d"));
        zs.add(5.0, b("e"));
        zs
    }

    #[test]
    fn test_new_zset_is_empty() {
        let zs = SortedSet::new();
        assert_eq!(zs.len(), 0);
        assert!(zs.is_empty());
    }

    #[test]
    fn test_add_new_member() {
        let mut zs = SortedSet::new();
        assert!(zs.add(1.0, b("a")));
        assert_eq!(zs.len(), 1);
    }

    #[test]
    fn test_add_existing_member_with_new_score() {
        let mut zs = SortedSet::new();
        assert!(zs.add(1.0, b("a")));
        assert!(zs.add(2.0, b("a"))); // changed score
        assert_eq!(zs.len(), 1);
        assert_eq!(zs.get_score(&b("a")), Some(2.0));
    }

    #[test]
    fn test_add_existing_member_with_same_score_returns_false() {
        let mut zs = SortedSet::new();
        zs.add(1.0, b("a"));
        assert!(!zs.add(1.0, b("a")));
    }

    #[test]
    fn test_remove_existing_member() {
        let mut zs = make_zset();
        assert!(zs.remove(&b("c")));
        assert_eq!(zs.len(), 4);
        assert!(!zs.contains_member(&b("c")));
    }

    #[test]
    fn test_remove_nonexistent_member() {
        let mut zs = make_zset();
        assert!(!zs.remove(&b("zzz")));
        assert_eq!(zs.len(), 5);
    }

    #[test]
    fn test_increment_score_new_member() {
        let mut zs = SortedSet::new();
        let new_score = zs.increment_score(&b("a"), 5.0);
        assert_eq!(new_score, 5.0);
        assert_eq!(zs.get_score(&b("a")), Some(5.0));
    }

    #[test]
    fn test_increment_score_existing_member() {
        let mut zs = make_zset();
        let new_score = zs.increment_score(&b("a"), 10.0);
        assert_eq!(new_score, 11.0);
        assert_eq!(zs.get_score(&b("a")), Some(11.0));
    }

    #[test]
    fn test_get_rank_ascending() {
        let zs = make_zset();
        assert_eq!(zs.get_rank(&b("a")), Some(0));
        assert_eq!(zs.get_rank(&b("c")), Some(2));
        assert_eq!(zs.get_rank(&b("e")), Some(4));
        assert_eq!(zs.get_rank(&b("missing")), None);
    }

    #[test]
    fn test_get_rev_rank_descending() {
        let zs = make_zset();
        assert_eq!(zs.get_rev_rank(&b("a")), Some(4));
        assert_eq!(zs.get_rev_rank(&b("c")), Some(2));
        assert_eq!(zs.get_rev_rank(&b("e")), Some(0));
    }

    #[test]
    fn test_pop_first_returns_min() {
        let mut zs = make_zset();
        let entry = zs.pop_first().unwrap();
        assert_eq!(entry.member, b("a"));
        assert_eq!(entry.score, 1.0);
        assert_eq!(zs.len(), 4);
    }

    #[test]
    fn test_pop_last_returns_max() {
        let mut zs = make_zset();
        let entry = zs.pop_last().unwrap();
        assert_eq!(entry.member, b("e"));
        assert_eq!(entry.score, 5.0);
        assert_eq!(zs.len(), 4);
    }

    #[test]
    fn test_pop_empty_returns_none() {
        let mut zs = SortedSet::new();
        assert!(zs.pop_first().is_none());
        assert!(zs.pop_last().is_none());
    }

    #[test]
    fn test_get_range_full() {
        let zs = make_zset();
        let entries = zs.get_range(0, -1);
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].member, b("a"));
        assert_eq!(entries[4].member, b("e"));
    }

    #[test]
    fn test_get_range_partial() {
        let zs = make_zset();
        let entries = zs.get_range(1, 3);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].member, b("b"));
        assert_eq!(entries[2].member, b("d"));
    }

    #[test]
    fn test_get_range_negative_indices() {
        let zs = make_zset();
        let entries = zs.get_range(-2, -1);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].member, b("d"));
        assert_eq!(entries[1].member, b("e"));
    }

    #[test]
    fn test_get_range_empty_zset() {
        let zs = SortedSet::new();
        assert!(zs.get_range(0, -1).is_empty());
    }

    #[test]
    fn test_get_range_out_of_bounds() {
        let zs = make_zset();
        // start > stop after normalization
        assert!(zs.get_range(3, 1).is_empty());
    }

    #[test]
    fn test_get_rev_range() {
        let zs = make_zset();
        let entries = zs.get_rev_range(0, 2);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].member, b("e"));
        assert_eq!(entries[2].member, b("c"));
    }

    #[test]
    fn test_get_range_by_score_inclusive() {
        let zs = make_zset();
        let entries =
            zs.get_range_by_score(ScoreBoundary::Inclusive(2.0), ScoreBoundary::Inclusive(4.0));
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].member, b("b"));
        assert_eq!(entries[2].member, b("d"));
    }

    #[test]
    fn test_get_range_by_score_exclusive() {
        let zs = make_zset();
        // 2 < score < 4
        let entries =
            zs.get_range_by_score(ScoreBoundary::Exclusive(2.0), ScoreBoundary::Exclusive(4.0));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].member, b("c"));
    }

    #[test]
    fn test_get_range_by_score_neg_infinity() {
        let zs = make_zset();
        let entries =
            zs.get_range_by_score(ScoreBoundary::NegInfinity, ScoreBoundary::Inclusive(2.0));
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_get_range_by_score_pos_infinity_returns_empty() {
        // PosInfinity as min: short-circuit
        let zs = make_zset();
        let entries = zs.get_range_by_score(ScoreBoundary::PosInfinity, ScoreBoundary::PosInfinity);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_remove_range_by_score() {
        let mut zs = make_zset();
        let removed =
            zs.remove_range_by_score(ScoreBoundary::Inclusive(2.0), ScoreBoundary::Inclusive(3.0));
        assert_eq!(removed, 2);
        assert_eq!(zs.len(), 3);
        assert!(!zs.contains_member(&b("b")));
        assert!(!zs.contains_member(&b("c")));
    }

    #[test]
    fn test_get_range_by_lex() {
        // All members must have the same score for lex queries to be meaningful.
        let mut zs = SortedSet::new();
        for m in ["alpha", "beta", "delta", "gamma"] {
            zs.add(0.0, b(m));
        }
        // Inclusive [beta, gamma] spans beta, delta, gamma (delta is lex-less than gamma).
        let entries = zs.get_range_by_lex(
            &LexBoundary::Inclusive(b("beta")),
            &LexBoundary::Inclusive(b("gamma")),
        );
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].member, b("beta"));
        assert_eq!(entries[1].member, b("delta"));
        assert_eq!(entries[2].member, b("gamma"));
    }

    #[test]
    fn test_get_range_by_lex_exclusive() {
        let mut zs = SortedSet::new();
        for m in ["a", "b", "c"] {
            zs.add(0.0, b(m));
        }
        let entries = zs.get_range_by_lex(
            &LexBoundary::Exclusive(b("a")),
            &LexBoundary::Exclusive(b("c")),
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].member, b("b"));
    }

    #[test]
    fn test_remove_range_by_rank() {
        let mut zs = make_zset();
        let removed = zs.remove_range_by_rank(1, 3);
        assert_eq!(removed, 3);
        assert_eq!(zs.len(), 2);
        assert!(zs.contains_member(&b("a")));
        assert!(zs.contains_member(&b("e")));
    }

    #[test]
    fn test_remove_range_by_rank_empty() {
        let mut zs = SortedSet::new();
        assert_eq!(zs.remove_range_by_rank(0, 5), 0);
    }

    #[test]
    fn test_remove_range_by_rank_negative() {
        let mut zs = make_zset();
        // Remove last two: d and e
        let removed = zs.remove_range_by_rank(-2, -1);
        assert_eq!(removed, 2);
        assert_eq!(zs.len(), 3);
    }

    #[test]
    fn test_scores_are_all_equal_edge_cases() {
        let mut zs = SortedSet::new();
        assert!(zs.scores_are_all_equal()); // empty
        zs.add(1.0, b("a"));
        assert!(zs.scores_are_all_equal()); // single
        zs.add(1.0, b("b"));
        assert!(zs.scores_are_all_equal()); // all 1.0
        zs.add(2.0, b("c"));
        assert!(!zs.scores_are_all_equal()); // mixed
    }

    #[test]
    fn test_memory_usage_sums_member_bytes() {
        let mut zs = SortedSet::new();
        zs.add(1.0, b("a"));
        zs.add(2.0, b("bb"));
        zs.add(3.0, b("ccc"));
        // member bytes total: 1 + 2 + 3 = 6
        // plus 3 * 8 bytes for f64 scores = 24
        let mem = zs.memory_usage();
        assert_eq!(mem, 6 + 3 * std::mem::size_of::<f64>());
    }

    #[test]
    fn test_sort_by_score_then_member() {
        // Same score: members are ordered lexicographically.
        let mut zs = SortedSet::new();
        zs.add(1.0, b("c"));
        zs.add(1.0, b("a"));
        zs.add(1.0, b("b"));
        let entries: Vec<_> = zs.iter().map(|e| e.member.clone()).collect();
        assert_eq!(entries, vec![b("a"), b("b"), b("c")]);
    }

    #[test]
    fn test_iter_returns_sorted_entries() {
        let zs = make_zset();
        let entries: Vec<_> = zs.iter().map(|e| (e.score, e.member.clone())).collect();
        assert_eq!(
            entries,
            vec![
                (1.0, b("a")),
                (2.0, b("b")),
                (3.0, b("c")),
                (4.0, b("d")),
                (5.0, b("e")),
            ]
        );
    }

    #[test]
    fn test_score_boundary_display() {
        assert_eq!(ScoreBoundary::NegInfinity.to_string(), "-inf");
        assert_eq!(ScoreBoundary::PosInfinity.to_string(), "+inf");
        assert_eq!(ScoreBoundary::Inclusive(1.5).to_string(), "1.5");
        assert_eq!(ScoreBoundary::Exclusive(2.5).to_string(), "(2.5");
    }

    #[test]
    fn test_lex_boundary_display() {
        assert_eq!(LexBoundary::Min.to_string(), "-");
        assert_eq!(LexBoundary::Max.to_string(), "+");
        assert_eq!(LexBoundary::Inclusive(b("foo")).to_string(), "[foo");
        assert_eq!(LexBoundary::Exclusive(b("bar")).to_string(), "(bar");
    }
}
