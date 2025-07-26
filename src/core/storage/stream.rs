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
