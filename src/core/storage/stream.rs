// src/core/storage/stream.rs

use bytes::Bytes;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// --- Stream ID ---
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Default, Serialize, Deserialize,
)]
pub struct StreamId {
    pub timestamp_ms: u64,
    pub sequence: u64,
}

impl StreamId {
    pub fn new(timestamp_ms: u64, sequence: u64) -> Self {
        Self {
            timestamp_ms,
            sequence,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StreamIdParseError(&'static str);

impl fmt::Display for StreamIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for StreamId {
    type Err = StreamIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "0" {
            return Ok(StreamId::new(0, 0));
        }

        let parts: Vec<&str> = s.split('-').collect();
        match parts.len() {
            1 => {
                let timestamp_ms = parts[0]
                    .parse()
                    .map_err(|_| StreamIdParseError("Invalid timestamp"))?;
                Ok(StreamId::new(timestamp_ms, 0))
            }
            2 => {
                let timestamp_ms = parts[0]
                    .parse()
                    .map_err(|_| StreamIdParseError("Invalid timestamp"))?;
                let sequence = parts[1]
                    .parse()
                    .map_err(|_| StreamIdParseError("Invalid sequence"))?;
                Ok(StreamId::new(timestamp_ms, sequence))
            }
            _ => Err(StreamIdParseError("Invalid Stream ID format")),
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.timestamp_ms, self.sequence)
    }
}

// --- Stream Entry ---
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: IndexMap<Bytes, Bytes>,
}

impl StreamEntry {
    pub fn memory_usage(&self) -> usize {
        self.fields
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
    }
}

// --- Consumer & Group State ---
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEntryInfo {
    pub consumer_name: Bytes,
    pub delivery_count: u64,
    pub delivery_time_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub pending_ids: BTreeSet<StreamId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub consumers: HashMap<Bytes, Consumer>,
    pub pending_entries: BTreeMap<StreamId, PendingEntryInfo>,

    // Indeks sekunder untuk pencarian idle entries yang efisien.
    // Tidak perlu dipersist karena bisa direkonstruksi saat startup atau tidak sama sekali.
    #[serde(skip)]
    pub idle_index: BTreeSet<(u64, StreamId)>,
}

// --- Main Stream Struct ---
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stream {
    pub entries: BTreeMap<StreamId, StreamEntry>,
    pub length: u64,
    pub last_generated_id: StreamId,
    pub groups: HashMap<Bytes, ConsumerGroup>,
    pub maxlen: Option<usize>,
    pub maxlen_is_approximate: bool,
    #[serde(skip)]
    pub sequence_number: Arc<AtomicU64>,
}

impl PartialEq for Stream {
    fn eq(&self, other: &Self) -> bool {
        // Bandingkan semua field kecuali `sequence_number`
        self.entries == other.entries
            && self.length == other.length
            && self.last_generated_id == other.last_generated_id
            && self.groups == other.groups
            && self.maxlen == other.maxlen
            && self.maxlen_is_approximate == other.maxlen_is_approximate
    }
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            length: 0,
            last_generated_id: StreamId::default(),
            groups: HashMap::new(),
            maxlen: None,
            maxlen_is_approximate: false,
            sequence_number: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Stream {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_entry(
        &mut self,
        id_spec: Option<StreamId>,
        fields: IndexMap<Bytes, Bytes>,
    ) -> Result<StreamId, &'static str> {
        let new_id = match id_spec {
            Some(id) => {
                if id == StreamId::new(0, 0) {
                    return Err("ERR The ID specified in XADD must be greater than 0-0");
                }
                if id <= self.last_generated_id {
                    return Err(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                id
            }
            None => {
                let mut timestamp_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if timestamp_ms <= self.last_generated_id.timestamp_ms {
                    timestamp_ms = self.last_generated_id.timestamp_ms;
                }

                let sequence = if timestamp_ms == self.last_generated_id.timestamp_ms {
                    self.last_generated_id.sequence + 1
                } else {
                    0
                };
                StreamId::new(timestamp_ms, sequence)
            }
        };

        let entry = StreamEntry { id: new_id, fields };
        self.entries.insert(new_id, entry);
        self.last_generated_id = new_id;
        self.length += 1;

        self.sequence_number.fetch_add(1, Ordering::Relaxed);

        Ok(new_id)
    }

    pub fn trim(&mut self) {
        if let Some(maxlen) = self.maxlen {
            while self.length as usize > maxlen {
                if let Some(key) = self.entries.keys().next().cloned() {
                    self.entries.remove(&key);
                    self.length -= 1;
                } else {
                    break;
                }
            }
        }
    }

    pub fn delete(&mut self, ids: &BTreeSet<StreamId>) -> usize {
        let mut deleted_count = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                deleted_count += 1;
                self.length -= 1;
            }
        }
        deleted_count
    }

    pub fn memory_usage(&self) -> usize {
        let entries_mem: usize = self.entries.values().map(|e| e.memory_usage()).sum();
        let groups_mem: usize = self
            .groups
            .values()
            .map(|g| {
                let consumers_mem: usize = g
                    .consumers
                    .values()
                    .map(|c| c.name.len() + c.pending_ids.len() * std::mem::size_of::<StreamId>())
                    .sum();
                g.name.len()
                    + consumers_mem
                    + g.pending_entries.len()
                        * (std::mem::size_of::<StreamId>()
                            + std::mem::size_of::<PendingEntryInfo>())
            })
            .sum();
        entries_mem + groups_mem
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    fn fields(pairs: &[(&[u8], &[u8])]) -> IndexMap<Bytes, Bytes> {
        pairs
            .iter()
            .map(|(k, v)| (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v)))
            .collect()
    }

    fn fields_str(pairs: &[(&str, &str)]) -> IndexMap<Bytes, Bytes> {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    Bytes::copy_from_slice(k.as_bytes()),
                    Bytes::copy_from_slice(v.as_bytes()),
                )
            })
            .collect()
    }

    #[test]
    fn test_stream_id_new_and_compare() {
        let a = StreamId::new(100, 0);
        let b = StreamId::new(100, 1);
        let c = StreamId::new(101, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn test_stream_id_parse_only_timestamp() {
        let id = "12345".parse::<StreamId>().unwrap();
        assert_eq!(id, StreamId::new(12345, 0));
    }

    #[test]
    fn test_stream_id_parse_with_sequence() {
        let id = "12345-7".parse::<StreamId>().unwrap();
        assert_eq!(id, StreamId::new(12345, 7));
    }

    #[test]
    fn test_stream_id_parse_special_zero() {
        let id = "0".parse::<StreamId>().unwrap();
        assert_eq!(id, StreamId::new(0, 0));
    }

    #[test]
    fn test_stream_id_parse_invalid() {
        assert!("abc".parse::<StreamId>().is_err());
        assert!("123-abc".parse::<StreamId>().is_err());
        assert!("1-2-3".parse::<StreamId>().is_err());
    }

    #[test]
    fn test_stream_id_display() {
        let id = StreamId::new(100, 5);
        assert_eq!(id.to_string(), "100-5");
    }

    #[test]
    fn test_new_stream_is_empty() {
        let s = Stream::new();
        assert_eq!(s.entries.len(), 0);
        assert_eq!(s.length, 0);
        assert!(s.groups.is_empty());
    }

    #[test]
    fn test_add_entry_explicit_id() {
        let mut s = Stream::new();
        let id = s
            .add_entry(Some(StreamId::new(1000, 0)), fields_str(&[("k", "v")]))
            .unwrap();
        assert_eq!(id, StreamId::new(1000, 0));
        assert_eq!(s.length, 1);
        assert_eq!(s.last_generated_id, StreamId::new(1000, 0));
    }

    #[test]
    fn test_add_entry_zero_id_rejected() {
        let mut s = Stream::new();
        let r = s.add_entry(Some(StreamId::new(0, 0)), fields(&[]));
        assert!(r.is_err());
    }

    #[test]
    fn test_add_entry_id_not_strictly_greater_rejected() {
        let mut s = Stream::new();
        s.add_entry(Some(StreamId::new(1000, 0)), fields(&[]))
            .unwrap();
        // Same id
        assert!(
            s.add_entry(Some(StreamId::new(1000, 0)), fields(&[]))
                .is_err()
        );
        // Lower timestamp
        assert!(
            s.add_entry(Some(StreamId::new(500, 0)), fields(&[]))
                .is_err()
        );
        // Same timestamp, lower sequence
        assert!(
            s.add_entry(Some(StreamId::new(1000, 0)), fields(&[]))
                .is_err()
        );
    }

    #[test]
    fn test_add_entry_auto_id_increments_sequence() {
        let mut s = Stream::new();
        // Pin last_generated_id to a timestamp in the far future so the
        // auto-id path always takes the "increment sequence" branch,
        // regardless of `SystemTime::now()`.
        s.last_generated_id = StreamId::new(u64::MAX, 5);
        let id = s.add_entry(None, fields(&[])).unwrap();
        assert_eq!(id.timestamp_ms, u64::MAX);
        assert_eq!(id.sequence, 6);
    }

    #[test]
    fn test_add_entry_bumps_sequence_number() {
        let mut s = Stream::new();
        s.last_generated_id = StreamId::new(2000, 0);
        let before = s.sequence_number.load(Ordering::Relaxed);
        s.add_entry(None, fields(&[])).unwrap();
        let after = s.sequence_number.load(Ordering::Relaxed);
        assert_eq!(after, before + 1);
    }

    #[test]
    fn test_trim_respects_maxlen() {
        let mut s = Stream::new();
        s.maxlen = Some(3);
        for i in 1..=5u64 {
            s.add_entry(Some(StreamId::new(i, 0)), fields(&[])).unwrap();
            s.trim();
        }
        assert_eq!(s.length, 3);
        // Oldest 2 entries should be gone.
        assert!(!s.entries.contains_key(&StreamId::new(1, 0)));
        assert!(!s.entries.contains_key(&StreamId::new(2, 0)));
        assert!(s.entries.contains_key(&StreamId::new(5, 0)));
    }

    #[test]
    fn test_trim_with_no_maxlen_is_noop() {
        let mut s = Stream::new();
        s.add_entry(Some(StreamId::new(1, 0)), fields(&[])).unwrap();
        s.add_entry(Some(StreamId::new(2, 0)), fields(&[])).unwrap();
        let len_before = s.length;
        s.trim();
        assert_eq!(s.length, len_before);
    }

    #[test]
    fn test_delete_returns_count() {
        let mut s = Stream::new();
        s.add_entry(Some(StreamId::new(1, 0)), fields(&[])).unwrap();
        s.add_entry(Some(StreamId::new(2, 0)), fields(&[])).unwrap();
        s.add_entry(Some(StreamId::new(3, 0)), fields(&[])).unwrap();
        let to_delete: BTreeSet<_> = [StreamId::new(1, 0), StreamId::new(3, 0)]
            .into_iter()
            .collect();
        assert_eq!(s.delete(&to_delete), 2);
        assert_eq!(s.length, 1);
    }

    #[test]
    fn test_delete_nonexistent_returns_zero() {
        let mut s = Stream::new();
        s.add_entry(Some(StreamId::new(1, 0)), fields(&[])).unwrap();
        let to_delete: BTreeSet<_> = [StreamId::new(99, 0)].into_iter().collect();
        assert_eq!(s.delete(&to_delete), 0);
    }

    #[test]
    fn test_memory_usage_grows_with_entries() {
        let mut s = Stream::new();
        let m0 = s.memory_usage();
        s.last_generated_id = StreamId::new(1, 0);
        s.add_entry(None, fields_str(&[("k", "value")])).unwrap();
        let m1 = s.memory_usage();
        assert!(m1 > m0, "memory must grow after adding an entry");
    }

    #[test]
    fn test_stream_entry_memory_usage() {
        let entry = StreamEntry {
            id: StreamId::new(1, 0),
            fields: fields_str(&[("foo", "bar")]),
        };
        // foo(3) + bar(3) = 6
        assert_eq!(entry.memory_usage(), 6);
    }
}
