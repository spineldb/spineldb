// src/core/storage/data_types.rs

//! Defines the core data structures for storing values in the database,
//! such as `StoredValue` and the `DataValue` enum.

use super::bloom::BloomFilter;
pub use super::cache_types::{CacheBody, VariantMap};
use super::hll::HyperLogLog;
use super::vector::SpinelVector;
use crate::core::Command;
use crate::core::commands::cache::cache_set::CacheSet as CacheSetCmd;
use crate::core::commands::cache::command::CacheSubcommand;
use crate::core::commands::generic;
use crate::core::commands::hash;
use crate::core::commands::json::Json;
use crate::core::commands::json::command::JsonSubcommand;
use crate::core::commands::json::json_set::JsonSet as JsonSetCmdInternal;
use crate::core::commands::list;
use crate::core::commands::set;
use crate::core::commands::streams;
use crate::core::commands::string;
use crate::core::commands::zset;
use crate::core::database::zset::SortedSet;
use crate::core::storage::stream::Stream;
use bytes::Bytes;
use indexmap::IndexMap;
use serde_json;
use std::collections::{HashSet, VecDeque};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// A hard limit on the size of a single string value to prevent DoS via excessive allocation.
pub const MAX_STRING_SIZE: usize = 512 * 1024 * 1024; // 512MB

/// Stores metadata for the LFU (Least Frequently Used) eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LfuInfo {
    /// Stores the last access time in minutes since the Unix epoch (16 bits).
    pub(crate) last_decrement_time: u32,
    /// The 8-bit logarithmic frequency counter.
    pub(crate) counter: u8,
}

impl Default for LfuInfo {
    fn default() -> Self {
        Self {
            last_decrement_time: lfu_time_now(),
            counter: LFU_INIT_VAL,
        }
    }
}

/// A wrapper for all values stored in the database, containing the data and metadata.
#[derive(Debug, Clone)]
pub struct StoredValue {
    pub data: DataValue,
    /// The time at which the value is considered stale (freshness TTL).
    pub expiry: Option<Instant>,
    /// The time when the stale-while-revalidate period ends.
    pub stale_revalidate_expiry: Option<Instant>,
    /// The time when the grace period ends (serve stale if origin is down).
    pub grace_expiry: Option<Instant>,
    /// Version for optimistic locking (`WATCH`).
    pub version: u64,
    /// The calculated size of the `data` field in bytes (for in-memory data).
    pub size: usize,
    /// LFU eviction policy metadata.
    pub lfu: LfuInfo,
}

impl StoredValue {
    /// Creates a new `StoredValue` with default metadata.
    pub fn new(data: DataValue) -> Self {
        let size = data.memory_usage();
        Self {
            data,
            expiry: None,
            stale_revalidate_expiry: None,
            grace_expiry: None,
            version: 1,
            size,
            lfu: LfuInfo::default(),
        }
    }

    /// Updates LFU metadata upon key access.
    pub fn update_lfu(&mut self) {
        let counter = self.lfu.counter;
        let now = lfu_time_now();
        let decay_periods = lfu_time_decay(now, self.lfu.last_decrement_time);

        let new_counter = if decay_periods > 0 {
            if decay_periods >= (counter as u32) {
                0
            } else {
                counter - decay_periods as u8
            }
        } else {
            counter
        };

        self.lfu.counter = lfu_log_incr(new_counter);
        self.lfu.last_decrement_time = now;
    }

    /// Calculates the remaining time-to-live in seconds.
    pub fn remaining_ttl_secs(&self) -> Option<u64> {
        self.expiry
            .and_then(|expiry| expiry.checked_duration_since(Instant::now()))
            .map(|d| d.as_secs())
    }

    /// Calculates the remaining time-to-live in milliseconds.
    pub fn remaining_ttl_ms(&self) -> Option<i64> {
        self.expiry
            .and_then(|expiry| expiry.checked_duration_since(Instant::now()))
            .map(|d| d.as_millis() as i64)
    }

    /// Checks if the value is expired based on its type.
    pub fn is_expired(&self) -> bool {
        let now = Instant::now();
        match self.data {
            DataValue::HttpCache { .. } => {
                self.grace_expiry.is_some_and(|exp| exp <= now)
                    || (self.grace_expiry.is_none() && self.expiry.is_some_and(|exp| exp <= now))
            }
            _ => self.expiry.is_some_and(|expiry| expiry <= now),
        }
    }

    /// Returns the total memory usage of this stored value in bytes.
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.size
    }

    /// Converts a `StoredValue` into a series of commands to reconstruct it for AOF/SPLDB.
    pub fn to_construction_commands(&self, key: &Bytes) -> Vec<Command> {
        const CHUNK_SIZE: usize = 50;
        let mut commands = Vec::new();
        let ttl_secs = self.remaining_ttl_secs();

        let base_commands: Vec<Command> = match &self.data {
            DataValue::String(value) => {
                let ttl_option =
                    ttl_secs.map_or(string::TtlOption::None, string::TtlOption::Seconds);
                vec![Command::Set(string::Set {
                    key: key.clone(),
                    value: value.clone(),
                    ttl: ttl_option,
                    condition: string::SetCondition::None,
                    get: false,
                })]
            }
            DataValue::List(items) => {
                if items.is_empty() {
                    return vec![];
                }
                items
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .chunks(CHUNK_SIZE)
                    .map(|chunk| {
                        Command::RPush(list::RPush {
                            key: key.clone(),
                            values: chunk.to_vec(),
                        })
                    })
                    .collect()
            }
            DataValue::Hash(fields) => {
                if fields.is_empty() {
                    return vec![];
                }
                fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>()
                    .chunks(CHUNK_SIZE)
                    .map(|chunk| {
                        Command::HSet(hash::HSet {
                            key: key.clone(),
                            fields: chunk.to_vec(),
                        })
                    })
                    .collect()
            }
            DataValue::Set(members) => {
                if members.is_empty() {
                    return vec![];
                }
                members
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .chunks(CHUNK_SIZE)
                    .map(|chunk| {
                        Command::Sadd(set::Sadd {
                            key: key.clone(),
                            members: chunk.to_vec(),
                        })
                    })
                    .collect()
            }
            DataValue::SortedSet(zset) => {
                let all_entries = zset.get_range(0, -1);
                if all_entries.is_empty() {
                    return vec![];
                }
                all_entries
                    .chunks(CHUNK_SIZE)
                    .map(|chunk| {
                        Command::Zadd(zset::Zadd {
                            key: key.clone(),
                            members: chunk.iter().map(|e| (e.score, e.member.clone())).collect(),
                            ..Default::default()
                        })
                    })
                    .collect()
            }
            DataValue::Stream(stream) => {
                let mut stream_commands = Vec::new();
                if stream.entries.is_empty() {
                    return stream_commands;
                }
                for entry in stream.entries.values() {
                    stream_commands.push(Command::XAdd(streams::XAdd::new_internal(
                        key.clone(),
                        Some(entry.id),
                        entry.fields.clone(),
                    )));
                }
                for group in stream.groups.values() {
                    stream_commands.push(Command::XGroup(streams::XGroup::new_create_internal(
                        key.clone(),
                        group.name.clone(),
                        group.last_delivered_id,
                        false,
                    )));
                }
                stream_commands
            }
            DataValue::Json(value) => {
                let json_string =
                    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
                vec![Command::Json(Json {
                    subcommand: JsonSubcommand::Set(JsonSetCmdInternal {
                        key: key.clone(),
                        path: ".".to_string(),
                        value_json_str: Bytes::from(json_string),
                        condition: Default::default(),
                    }),
                })]
            }
            DataValue::HyperLogLog(hll) => {
                vec![Command::Set(string::Set {
                    key: key.clone(),
                    value: hll.serialize(),
                    ttl: string::TtlOption::None, // TTL is handled by the generic EXPIRE command later
                    condition: string::SetCondition::None,
                    get: false,
                })]
            }
            DataValue::BloomFilter(bf) => {
                vec![Command::Set(string::Set {
                    key: key.clone(),
                    value: bf.serialize(),
                    ttl: string::TtlOption::None,
                    condition: string::SetCondition::None,
                    get: false,
                })]
            }
            DataValue::SpinelVector(v) => {
                vec![Command::Set(string::Set {
                    key: key.clone(),
                    value: v.serialize(),
                    ttl: string::TtlOption::None,
                    condition: string::SetCondition::None,
                    get: false,
                })]
            }
            DataValue::HttpCache {
                variants, vary_on, ..
            } => {
                let mut cache_commands = Vec::new();
                let vary_str = vary_on
                    .iter()
                    .map(|b| String::from_utf8_lossy(b))
                    .collect::<Vec<_>>()
                    .join(",");

                let now = Instant::now();
                let fresh_ttl = self
                    .expiry
                    .and_then(|exp| exp.checked_duration_since(now).map(|d| d.as_secs()));
                let swr_ttl =
                    self.stale_revalidate_expiry
                        .zip(self.expiry)
                        .and_then(|(swr_exp, exp)| {
                            swr_exp.checked_duration_since(exp).map(|d| d.as_secs())
                        });
                let grace_ttl = self
                    .grace_expiry
                    .zip(self.stale_revalidate_expiry)
                    .and_then(|(grace_exp, swr_exp)| {
                        grace_exp
                            .checked_duration_since(swr_exp)
                            .map(|d| d.as_secs())
                    });

                for variant in variants.values() {
                    let body_bytes = match &variant.body {
                        CacheBody::InMemory(bytes) => bytes.clone(),
                        // On-disk and negative caches are not persisted via AOF/SPLDB commands.
                        // They are reconstructed via their own mechanisms if needed.
                        _ => continue,
                    };

                    cache_commands.push(Command::Cache(crate::core::commands::cache::Cache {
                        subcommand: CacheSubcommand::Set(CacheSetCmd {
                            key: key.clone(),
                            body_data: body_bytes,
                            ttl: fresh_ttl,
                            swr: swr_ttl,
                            grace: grace_ttl,
                            revalidate_url: variant.metadata.revalidate_url.clone(),
                            etag: variant.metadata.etag.clone(),
                            last_modified: variant.metadata.last_modified.clone(),
                            tags: vec![],
                            vary: if vary_on.is_empty() {
                                None
                            } else {
                                Some(Bytes::from(vary_str.clone()))
                            },
                            headers: None,
                            compression: matches!(
                                variant.body,
                                CacheBody::CompressedInMemory { .. }
                            ),
                            force_disk: false, // This state is transient and not stored this way.
                        }),
                    }));
                }
                cache_commands
            }
        };
        commands.extend(base_commands);

        if !matches!(
            &self.data,
            DataValue::String(_) | DataValue::HttpCache { .. }
        ) && !commands.is_empty()
            && let Some(secs) = ttl_secs
            && secs > 0
        {
            commands.push(Command::Expire(generic::Expire {
                key: key.clone(),
                seconds: secs,
            }));
        }
        commands
    }
}

/// Recursively estimates the memory usage of a `serde_json::Value` without serialization.
fn estimate_json_memory(val: &serde_json::Value) -> usize {
    use serde_json::Value;
    match val {
        Value::Null | Value::Bool(_) => std::mem::size_of::<Value>(),
        Value::Number(n) => std::mem::size_of::<Value>() + n.to_string().len(),
        Value::String(s) => std::mem::size_of::<Value>() + s.capacity(),
        Value::Array(arr) => {
            std::mem::size_of::<Value>()
                + arr.capacity() * std::mem::size_of::<Value>()
                + arr.iter().map(estimate_json_memory).sum::<usize>()
        }
        Value::Object(map) => {
            std::mem::size_of::<Value>()
                + map
                    .iter()
                    .map(|(k, v)| k.capacity() + estimate_json_memory(v))
                    .sum::<usize>()
        }
    }
}

/// An enum representing the different data types that can be stored.
#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(IndexMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
    SortedSet(SortedSet),
    Stream(Stream),
    Json(serde_json::Value),
    HyperLogLog(Box<HyperLogLog>),
    BloomFilter(Box<BloomFilter>),
    SpinelVector(Box<SpinelVector>),
    HttpCache {
        variants: VariantMap,
        vary_on: Vec<Bytes>,
        /// The cluster-wide logical clock epoch when the tags were last set.
        tags_epoch: u64,
    },
}

impl DataValue {
    /// Calculates the memory usage of the data payload.
    pub fn memory_usage(&self) -> usize {
        match self {
            DataValue::String(b) => b.len(),
            DataValue::List(l) => {
                // Account for the collection's own allocation + the data within
                (l.capacity() * std::mem::size_of::<Bytes>())
                    + l.iter().map(|b| b.len()).sum::<usize>()
            }
            DataValue::Hash(h) => {
                // Account for the collection's own allocation + the data within
                (h.capacity() * (std::mem::size_of::<Bytes>() + std::mem::size_of::<Bytes>()))
                    + h.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
            }
            DataValue::Set(s) => {
                // Account for the collection's own allocation + the data within
                (s.capacity() * std::mem::size_of::<Bytes>())
                    + s.iter().map(|b| b.len()).sum::<usize>()
            }
            DataValue::SortedSet(z) => z.memory_usage(),
            DataValue::Stream(s) => s.memory_usage(),
            DataValue::Json(v) => estimate_json_memory(v),
            DataValue::HyperLogLog(hll) => hll.memory_usage(),
            DataValue::BloomFilter(bf) => bf.memory_usage(),
            DataValue::SpinelVector(v) => v.memory_usage(),
            DataValue::HttpCache {
                variants, vary_on, ..
            } => {
                let vary_headers_size: usize = vary_on.iter().map(|b| b.len()).sum();
                let variants_size: usize = variants
                    .values()
                    .map(|variant| {
                        let meta_size = variant.metadata.memory_usage();
                        let body_size = match &variant.body {
                            CacheBody::InMemory(b) => b.len(),
                            CacheBody::CompressedInMemory { data, .. } => data.len(),
                            CacheBody::Negative { body, .. } => {
                                body.as_ref().map_or(0, |b| b.len())
                            }
                            CacheBody::OnDisk { .. } => 0, // On-disk does not count towards RAM usage
                        };
                        body_size + meta_size
                    })
                    .sum();
                vary_headers_size + variants_size + std::mem::size_of::<u64>()
            }
        }
    }
}

// LFU Helper Constants and Functions
const LFU_INIT_VAL: u8 = 5;
const LFU_DECAY_TIME_MINUTES: u32 = 1;
const LFU_LOG_FACTOR: f64 = 10.0;

fn lfu_time_now() -> u32 {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 60) as u32
}

fn lfu_time_decay(now: u32, last_access: u32) -> u32 {
    now.saturating_sub(last_access) / LFU_DECAY_TIME_MINUTES
}

fn lfu_log_incr(counter: u8) -> u8 {
    if counter == 255 {
        return 255;
    }
    let r: f64 = rand::random();
    let baseval = counter.saturating_sub(LFU_INIT_VAL) as f64;
    let p = 1.0 / (baseval * LFU_LOG_FACTOR + 1.0);
    if r < p {
        counter.saturating_add(1)
    } else {
        counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::storage::cache_types::{CacheVariant, HttpMetadata};
    use std::time::Duration;

    #[test]
    fn test_lfu_info_default() {
        let lfu = LfuInfo::default();
        assert_eq!(lfu.counter, LFU_INIT_VAL);
    }

    #[test]
    fn test_stored_value_new_default_metadata() {
        let sv = StoredValue::new(DataValue::String(Bytes::from_static(b"hello")));
        assert!(sv.expiry.is_none());
        assert!(sv.stale_revalidate_expiry.is_none());
        assert!(sv.grace_expiry.is_none());
        assert_eq!(sv.version, 1);
        assert_eq!(sv.size, 5);
        assert_eq!(sv.lfu.counter, LFU_INIT_VAL);
    }

    #[test]
    fn test_stored_value_not_expired_without_expiry() {
        let sv = StoredValue::new(DataValue::String(Bytes::from_static(b"x")));
        assert!(!sv.is_expired());
        assert!(sv.remaining_ttl_secs().is_none());
        assert!(sv.remaining_ttl_ms().is_none());
    }

    #[test]
    fn test_stored_value_not_expired_when_future_ttl() {
        let mut sv = StoredValue::new(DataValue::String(Bytes::from_static(b"x")));
        sv.expiry = Some(Instant::now() + Duration::from_secs(60));
        assert!(!sv.is_expired());
        let secs = sv.remaining_ttl_secs().unwrap();
        // Allow a small tolerance (allow 60 or 59 due to scheduling).
        assert!(secs <= 60);
    }

    #[test]
    fn test_stored_value_expired_when_past_ttl() {
        let mut sv = StoredValue::new(DataValue::String(Bytes::from_static(b"x")));
        sv.expiry = Some(Instant::now() - Duration::from_secs(1));
        assert!(sv.is_expired());
        assert!(sv.remaining_ttl_secs().is_none());
        assert!(sv.remaining_ttl_ms().is_none());
    }

    #[test]
    fn test_stored_value_http_cache_expired_via_grace() {
        // HttpCache uses grace_expiry, not expiry.
        let sv = StoredValue {
            data: DataValue::HttpCache {
                variants: VariantMap::new(),
                vary_on: vec![],
                tags_epoch: 0,
            },
            expiry: None,
            stale_revalidate_expiry: None,
            grace_expiry: Some(Instant::now() - Duration::from_secs(1)),
            version: 1,
            size: 0,
            lfu: LfuInfo::default(),
        };
        assert!(sv.is_expired());
    }

    #[test]
    fn test_stored_value_http_cache_not_expired_within_grace() {
        let sv = StoredValue {
            data: DataValue::HttpCache {
                variants: VariantMap::new(),
                vary_on: vec![],
                tags_epoch: 0,
            },
            expiry: None,
            stale_revalidate_expiry: None,
            grace_expiry: Some(Instant::now() + Duration::from_secs(60)),
            version: 1,
            size: 0,
            lfu: LfuInfo::default(),
        };
        assert!(!sv.is_expired());
    }

    #[test]
    fn test_stored_value_http_cache_expired_via_expiry_when_no_grace() {
        // HttpCache: if grace is None and expiry is past, treat as expired.
        let sv = StoredValue {
            data: DataValue::HttpCache {
                variants: VariantMap::new(),
                vary_on: vec![],
                tags_epoch: 0,
            },
            expiry: Some(Instant::now() - Duration::from_secs(1)),
            stale_revalidate_expiry: None,
            grace_expiry: None,
            version: 1,
            size: 0,
            lfu: LfuInfo::default(),
        };
        assert!(sv.is_expired());
    }

    #[test]
    fn test_stored_value_memory_usage_includes_overhead() {
        let sv = StoredValue::new(DataValue::String(Bytes::from_static(b"abcd")));
        // size_of(StoredValue) overhead + 4 bytes payload.
        let expected = std::mem::size_of::<StoredValue>() + 4;
        assert_eq!(sv.memory_usage(), expected);
    }

    #[test]
    fn test_stored_value_update_lfu_saturates_at_255() {
        let mut sv = StoredValue::new(DataValue::String(Bytes::from_static(b"x")));
        sv.lfu.counter = 255;
        // Calling update_lfu must not overflow.
        sv.update_lfu();
        assert_eq!(sv.lfu.counter, 255);
    }

    #[test]
    fn test_data_value_memory_string() {
        let dv = DataValue::String(Bytes::from_static(b"hello"));
        assert_eq!(dv.memory_usage(), 5);
    }

    #[test]
    fn test_data_value_memory_list() {
        let mut l = VecDeque::new();
        l.push_back(Bytes::from_static(b"a"));
        l.push_back(Bytes::from_static(b"bc"));
        // The memory formula accounts for the collection's capacity (not length),
        // which is at least the number of pushed elements.
        let expected = l.capacity() * std::mem::size_of::<Bytes>() + 3;
        let dv = DataValue::List(l);
        assert_eq!(dv.memory_usage(), expected);
    }

    #[test]
    fn test_data_value_memory_hash() {
        let mut h = IndexMap::new();
        h.insert(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        h.insert(Bytes::from_static(b"k2"), Bytes::from_static(b"v22"));
        let capacity_bytes =
            h.capacity() * (std::mem::size_of::<Bytes>() + std::mem::size_of::<Bytes>());
        let data_bytes: usize = h.iter().map(|(k, v)| k.len() + v.len()).sum();
        let expected = capacity_bytes + data_bytes;
        let dv = DataValue::Hash(h);
        assert_eq!(dv.memory_usage(), expected);
    }

    #[test]
    fn test_data_value_memory_set() {
        let mut s = HashSet::new();
        s.insert(Bytes::from_static(b"x"));
        s.insert(Bytes::from_static(b"yy"));
        let expected = s.capacity() * std::mem::size_of::<Bytes>() + 1 + 2;
        let dv = DataValue::Set(s);
        assert_eq!(dv.memory_usage(), expected);
    }

    #[test]
    fn test_data_value_memory_json_null() {
        let dv = DataValue::Json(serde_json::Value::Null);
        assert_eq!(dv.memory_usage(), std::mem::size_of::<serde_json::Value>());
    }

    #[test]
    fn test_data_value_memory_json_string() {
        let dv = DataValue::Json(serde_json::Value::String("hi".to_string()));
        let s = std::mem::size_of::<serde_json::Value>() + "hi".len();
        assert_eq!(dv.memory_usage(), s);
    }

    #[test]
    fn test_data_value_memory_http_cache_includes_vary() {
        let mut variants = VariantMap::new();
        variants.insert(
            1,
            CacheVariant {
                metadata: HttpMetadata::default(),
                body: CacheBody::InMemory(Bytes::from_static(b"abcd")),
                last_accessed: Instant::now(),
            },
        );
        let meta_size = variants.values().next().unwrap().metadata.memory_usage();
        let dv = DataValue::HttpCache {
            variants,
            vary_on: vec![Bytes::from_static(b"accept")],
            tags_epoch: 0,
        };
        // 6 (accept) + 4 (body) + 0 (default metadata) + 8 (tags_epoch)
        let expected = 6 + 4 + meta_size + std::mem::size_of::<u64>();
        assert_eq!(dv.memory_usage(), expected);
    }

    #[test]
    fn test_data_value_partial_eq_string() {
        let a = DataValue::String(Bytes::from_static(b"x"));
        let b = DataValue::String(Bytes::from_static(b"x"));
        let c = DataValue::String(Bytes::from_static(b"y"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_data_value_partial_eq_different_variants() {
        let a = DataValue::String(Bytes::from_static(b"x"));
        let b = DataValue::Json(serde_json::Value::Null);
        assert_ne!(a, b);
    }

    #[test]
    fn test_to_construction_commands_string_no_ttl() {
        let sv = StoredValue::new(DataValue::String(Bytes::from_static(b"hi")));
        let key = Bytes::from_static(b"mykey");
        let cmds = sv.to_construction_commands(&key);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Set(_)));
    }

    #[test]
    fn test_to_construction_commands_string_with_ttl_still_one_set() {
        // For DataValue::String the TTL is bundled into the SET command itself;
        // an additional EXPIRE is NOT appended (only non-String types get one).
        let mut sv = StoredValue::new(DataValue::String(Bytes::from_static(b"hi")));
        sv.expiry = Some(Instant::now() + Duration::from_secs(60));
        let key = Bytes::from_static(b"mykey");
        let cmds = sv.to_construction_commands(&key);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Set(_)));
    }

    #[test]
    fn test_to_construction_commands_list_with_ttl_adds_expire() {
        let mut l = VecDeque::new();
        l.push_back(Bytes::from_static(b"a"));
        let mut sv = StoredValue::new(DataValue::List(l));
        sv.expiry = Some(Instant::now() + Duration::from_secs(60));
        let key = Bytes::from_static(b"lst");
        let cmds = sv.to_construction_commands(&key);
        // 1 RPUSH + 1 EXPIRE
        assert_eq!(cmds.len(), 2);
        assert!(matches!(cmds[1], Command::Expire(_)));
    }

    #[test]
    fn test_to_construction_commands_list_chunks() {
        let mut l = VecDeque::new();
        for i in 0..120u8 {
            l.push_back(Bytes::copy_from_slice(&[i]));
        }
        let sv = StoredValue::new(DataValue::List(l));
        let key = Bytes::from_static(b"lst");
        let cmds = sv.to_construction_commands(&key);
        // 120 items / 50 per chunk = 3 RPUSH commands.
        assert_eq!(cmds.len(), 3);
    }

    #[test]
    fn test_to_construction_commands_empty_list_no_commands() {
        let sv = StoredValue::new(DataValue::List(VecDeque::new()));
        let key = Bytes::from_static(b"lst");
        let cmds = sv.to_construction_commands(&key);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_to_construction_commands_empty_hash_no_commands() {
        let sv = StoredValue::new(DataValue::Hash(IndexMap::new()));
        let key = Bytes::from_static(b"h");
        let cmds = sv.to_construction_commands(&key);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_to_construction_commands_empty_set_no_commands() {
        let sv = StoredValue::new(DataValue::Set(HashSet::new()));
        let key = Bytes::from_static(b"s");
        let cmds = sv.to_construction_commands(&key);
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_to_construction_commands_json() {
        let sv = StoredValue::new(DataValue::Json(serde_json::json!({"x": 1})));
        let key = Bytes::from_static(b"j");
        let cmds = sv.to_construction_commands(&key);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Json(_)));
    }

    #[test]
    fn test_to_construction_commands_hll() {
        let mut hll = HyperLogLog::new();
        hll.add(&Bytes::from_static(b"elem"), 0);
        let sv = StoredValue::new(DataValue::HyperLogLog(Box::new(hll)));
        let key = Bytes::from_static(b"hll");
        let cmds = sv.to_construction_commands(&key);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Set(_)));
    }

    #[test]
    fn test_to_construction_commands_bloom() {
        let bf = BloomFilter::new(100, 0.01);
        let sv = StoredValue::new(DataValue::BloomFilter(Box::new(bf)));
        let key = Bytes::from_static(b"bf");
        let cmds = sv.to_construction_commands(&key);
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Set(_)));
    }

    #[test]
    fn test_to_construction_commands_http_cache_with_ttl() {
        // HttpCache with InMemory variant and TTL should serialize correctly
        let mut variants = VariantMap::new();
        variants.insert(
            1,
            CacheVariant {
                body: CacheBody::InMemory(Bytes::from_static(b"test body")),
                metadata: HttpMetadata::default(),
                last_accessed: Instant::now(),
            },
        );
        let mut sv = StoredValue::new(DataValue::HttpCache {
            variants,
            vary_on: vec![],
            tags_epoch: 0,
        });
        sv.expiry = Some(Instant::now() + Duration::from_secs(60));
        sv.stale_revalidate_expiry = Some(Instant::now() + Duration::from_secs(90));
        sv.grace_expiry = Some(Instant::now() + Duration::from_secs(120));
        let key = Bytes::from_static(b"cache_key");
        let cmds = sv.to_construction_commands(&key);
        // HttpCache TTL is bundled into CACHE.SET command, no additional EXPIRE
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Cache(_)));
    }

    #[test]
    fn test_to_construction_commands_http_cache_empty_variants_returns_empty() {
        // HttpCache with no InMemory variants should return empty commands
        // (OnDisk and Negative are not persisted via AOF/SPLDB)
        let variants = VariantMap::new();
        let sv = StoredValue::new(DataValue::HttpCache {
            variants,
            vary_on: vec![],
            tags_epoch: 0,
        });
        let key = Bytes::from_static(b"cache_key");
        let cmds = sv.to_construction_commands(&key);
        assert!(cmds.is_empty());
    }
}
