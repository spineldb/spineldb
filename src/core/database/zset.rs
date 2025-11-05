// src/core/storage/zset.rs

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
        let to_remove: Vec<ZSetEntry> = self.get_range_by_score(min, max);
        let count = to_remove.len();
        if count > 0 {
            for entry in to_remove {
                self.remove(&entry.member);
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
        let to_remove = self.get_range_by_lex(min, max);
        let count = to_remove.len();
        if count > 0 {
            for entry in to_remove {
                self.remove(&entry.member);
            }
        }
        count
    }

    /// Removes entries within a rank range.
    pub fn remove_range_by_rank(&mut self, start: i64, stop: i64) -> usize {
        let to_remove: Vec<ZSetEntry> = self.get_range(start, stop);
        let count = to_remove.len();
        if count > 0 {
            for entry in to_remove {
                self.remove(&entry.member);
            }
        }
        count
    }
}
