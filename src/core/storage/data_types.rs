// src/core/storage/data_types.rs

//! Defines the core data structures for storing values in the database,
//! such as `StoredValue` and the `DataValue` enum.

pub use super::cache_types::{CacheBody, VariantMap};
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
use crate::core::storage::db::zset::SortedSet;
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
    pub(crate) last_decrement_time: u16,
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
            if decay_periods >= counter as u16 {
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
                    }),
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
            DataValue::List(l) => l.iter().map(|b| b.len()).sum(),
            DataValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len()).sum(),
            DataValue::Set(s) => s.iter().map(|b| b.len()).sum(),
            DataValue::SortedSet(z) => z.memory_usage(),
            DataValue::Stream(s) => s.memory_usage(),
            DataValue::Json(v) => estimate_json_memory(v),
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
const LFU_DECAY_TIME_MINUTES: u16 = 1;
const LFU_LOG_FACTOR: f64 = 10.0;

fn lfu_time_now() -> u16 {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 60) as u16
}

fn lfu_time_decay(now: u16, last_access: u16) -> u16 {
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
