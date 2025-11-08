// src/core/persistence/spldb.rs

//! Implements the SPLDB (SpinelDB Snapshot File) format for snapshot-based persistence.
//! This module handles both loading an SPLDB file from disk and saving the current
//! database state to an SPLDB file. It also provides helpers for serializing
//! single values, used by the MIGRATE/RESTORE commands.

use crate::core::SpinelDBError;
use crate::core::database::Db;
use crate::core::database::zset::SortedSet;
use crate::core::state::ServerState;
use crate::core::storage::cache_types::{CacheBody, CacheVariant, HttpMetadata};
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::storage::stream::{
    ConsumerGroup, PendingEntryInfo, Stream, StreamEntry, StreamId,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc::{CRC_64_REDIS, Crc};
use indexmap::IndexMap;
use ryu;
use serde_json;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{self, Error, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tracing::{debug, info, warn};

// --- SPLDB Constants ---
const SPLDB_MAGIC: &[u8] = b"SPINELDB";
const SPLDB_VERSION: &[u8] = b"0001";

const SPLDB_OPCODE_AUX: u8 = 0xFA;
const SPLDB_OPCODE_RESIZEDB: u8 = 0xFB;
const SPLDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const SPLDB_OPCODE_SELECTDB: u8 = 0xFE;
const SPLDB_OPCODE_EOF: u8 = 0xFF;

const SPLDB_TYPE_STRING: u8 = 0;
const SPLDB_TYPE_LIST: u8 = 1;
const SPLDB_TYPE_SET: u8 = 2;
const SPLDB_TYPE_ZSET: u8 = 3;
const SPLDB_TYPE_HASH: u8 = 4;
const SPLDB_TYPE_STREAM: u8 = 5;
const SPLDB_TYPE_JSON: u8 = 6;
const SPLDB_TYPE_HTTPCACHE: u8 = 7;
const SPLDB_TYPE_HYPERLOGLOG: u8 = 8;
const SPLDB_TYPE_BLOOMFILTER: u8 = 9;

const CHECKSUM_ALGO: Crc<u64> = Crc::<u64>::new(&CRC_64_REDIS);

// --- SPLDB Loader ---

pub struct SpldbLoader {
    config: crate::config::PersistenceConfig,
}

impl SpldbLoader {
    pub fn new(config: crate::config::PersistenceConfig) -> Self {
        Self { config }
    }

    /// Loads the main SPLDB file into the provided `ServerState` at startup.
    pub async fn load_into(&self, state: &Arc<ServerState>) -> Result<(), SpinelDBError> {
        let path = &self.config.spldb_path;
        info!("Attempting to load SPLDB from disk at {}", path);
        let metadata = match fs::metadata(path).await {
            Ok(m) => m,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                info!(
                    "SPLDB file not found at {}. Starting with an empty database.",
                    path
                );
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        if !metadata.is_file() || metadata.len() == 0 {
            info!(
                "SPLDB file at {} is empty or not a file. Starting fresh.",
                path
            );
            return Ok(());
        }

        let spldb_bytes = Bytes::from(fs::read(path).await?);
        info!(
            "SPLDB file found ({} bytes). Starting parsing...",
            spldb_bytes.len()
        );
        load_from_bytes(&spldb_bytes, &state.dbs).await?;
        info!("Successfully loaded database from SPLDB file {}", path);
        Ok(())
    }
}

/// A parser for processing an SPLDB file's byte stream.
struct SpldbParser<'a> {
    cursor: Bytes,
    dbs: &'a [Arc<Db>],
    current_db_index: usize,
    current_expiry: Option<Instant>,
}

impl<'a> SpldbParser<'a> {
    fn new(data: Bytes, dbs: &'a [Arc<Db>]) -> Self {
        Self {
            cursor: data,
            dbs,
            current_db_index: 0,
            current_expiry: None,
        }
    }

    /// Parses and validates the SPLDB header.
    fn parse_header(&mut self) -> io::Result<()> {
        if self.cursor.len() < SPLDB_MAGIC.len() + SPLDB_VERSION.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid SPLDB header length",
            ));
        }
        let magic = self.cursor.split_to(SPLDB_MAGIC.len());
        if magic != SPLDB_MAGIC {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid SPLDB magic string",
            ));
        }
        self.cursor.advance(SPLDB_VERSION.len());
        Ok(())
    }

    /// Parses the main body of the SPLDB file, processing opcodes and key-value pairs.
    async fn parse_kv_pairs(&mut self) -> io::Result<()> {
        loop {
            if !self.cursor.has_remaining() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "SPLDB data ended without EOF opcode",
                ));
            }

            let opcode = self.cursor.get_u8();

            match opcode {
                SPLDB_OPCODE_EOF => {
                    debug!("SPLDB EOF reached. Parsing complete.");
                    return Ok(());
                }
                SPLDB_OPCODE_AUX => {
                    read_string(&mut self.cursor)?;
                    read_string(&mut self.cursor)?;
                }
                SPLDB_OPCODE_SELECTDB => {
                    let db_index = read_length_encoding(&mut self.cursor)? as usize;
                    if db_index >= self.dbs.len() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!("SPLDB contains SELECT for out-of-range DB index {db_index}"),
                        ));
                    }
                    self.current_db_index = db_index;
                }
                SPLDB_OPCODE_RESIZEDB => {
                    read_length_encoding(&mut self.cursor)?;
                    read_length_encoding(&mut self.cursor)?;
                }
                SPLDB_OPCODE_EXPIRETIME_MS => {
                    let ts_ms = self.cursor.get_u64_le();
                    let expiry_time = UNIX_EPOCH + Duration::from_millis(ts_ms);
                    self.current_expiry = if let Ok(duration_from_now) =
                        expiry_time.duration_since(SystemTime::now())
                    {
                        Some(Instant::now() + duration_from_now)
                    } else {
                        None
                    };
                }
                value_type => self.parse_value(value_type).await?,
            }
        }
    }

    /// Parses a single key-value pair and inserts it into the database.
    async fn parse_value(&mut self, value_type: u8) -> io::Result<()> {
        let key = read_string(&mut self.cursor)?;
        let data_value = deserialize_single_value_data(&mut self.cursor, value_type)?;

        if self.current_expiry.is_some_and(|exp| exp <= Instant::now()) {
            self.current_expiry = None;
            return Ok(());
        }

        let mut stored_value = StoredValue::new(data_value);
        stored_value.expiry = self.current_expiry.take();

        if let Some(db) = self.dbs.get(self.current_db_index) {
            db.insert_value_from_load(key, stored_value).await;
        } else {
            warn!(
                "DB index {} from SPLDB is out of bounds for current server config. Skipping key.",
                self.current_db_index
            );
        }

        Ok(())
    }
}

/// Loads a full SPLDB file from a byte slice into the databases.
pub async fn load_from_bytes(data: &Bytes, dbs: &[Arc<Db>]) -> io::Result<()> {
    if data.len() < 8 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "SPLDB file is too short for checksum.",
        ));
    }

    let (data_part, checksum_part) = data.split_at(data.len() - 8);
    let expected_checksum = CHECKSUM_ALGO.checksum(data_part);
    let file_checksum = (&checksum_part[..]).get_u64_le();

    if expected_checksum != file_checksum {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "SPLDB checksum mismatch. File may be corrupt.",
        ));
    }
    info!("SPLDB checksum verified successfully.");

    // Clear all databases before loading data.
    for db in dbs.iter() {
        let guards = db.lock_all_shards().await;
        for mut guard in guards {
            guard.clear();
        }
    }

    let mut parser = SpldbParser::new(Bytes::from(data_part.to_vec()), dbs);
    parser.parse_header()?;
    parser.parse_kv_pairs().await?;

    Ok(())
}

/// Saves the current state of all databases to an SPLDB file at the given path.
pub async fn save(dbs: &[Arc<Db>], path: &str) -> io::Result<()> {
    info!("Starting SPLDB save to disk at {}", path);
    let temp_path_str = format!("{}.tmp.{}", path, rand::random::<u32>());
    let bytes = save_to_bytes(dbs).await?;
    fs::write(&temp_path_str, &bytes).await?;
    info!(
        "SPLDB snapshot successfully written to temporary file {}",
        temp_path_str
    );
    fs::rename(&temp_path_str, path).await?;
    info!("SPLDB file successfully saved to {}", path);
    Ok(())
}

/// Serializes the state of all databases into a single `Bytes` object in SPLDB format.
pub async fn save_to_bytes(dbs: &[Arc<Db>]) -> io::Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.put_slice(SPLDB_MAGIC);
    buf.put_slice(SPLDB_VERSION);

    buf.put_u8(SPLDB_OPCODE_AUX);
    write_string(&mut buf, b"spineldb-ver");
    write_string(&mut buf, env!("CARGO_PKG_VERSION").as_bytes());

    buf.put_u8(SPLDB_OPCODE_AUX);
    write_string(&mut buf, b"spineldb-bits");
    write_string(&mut buf, b"64");

    buf.put_u8(SPLDB_OPCODE_AUX);
    let ctime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    write_string(&mut buf, b"ctime");
    write_string(&mut buf, &ctime.to_string().into_bytes());

    for (db_index, db) in dbs.iter().enumerate() {
        let all_kvs = db.get_all_kvs_for_sync().await;
        let valid_kvs: Vec<_> = all_kvs
            .into_iter()
            .filter(|(_, val)| !val.is_expired())
            .collect();
        if valid_kvs.is_empty() {
            continue;
        }

        if db_index > 0 {
            buf.put_u8(SPLDB_OPCODE_SELECTDB);
            write_length_encoding(&mut buf, db_index as u64);
        }

        buf.put_u8(SPLDB_OPCODE_RESIZEDB);
        write_length_encoding(&mut buf, valid_kvs.len() as u64);
        write_length_encoding(&mut buf, 0);

        for (key, stored_value) in valid_kvs {
            write_kv(&mut buf, &key, &stored_value)?;
        }
    }

    buf.put_u8(SPLDB_OPCODE_EOF);
    let checksum = CHECKSUM_ALGO.checksum(&buf);
    buf.put_u64_le(checksum);
    Ok(buf.freeze())
}

/// Writes a single key-value pair, including its TTL if it exists.
fn write_kv(buf: &mut BytesMut, key: &Bytes, value: &StoredValue) -> io::Result<()> {
    if let Some(expiry) = value.expiry
        && let Some(duration) = expiry.checked_duration_since(Instant::now())
        && let Ok(now_ms) = SystemTime::now().duration_since(UNIX_EPOCH)
    {
        let expiry_ms = now_ms.as_millis() as u64 + duration.as_millis() as u64;
        buf.put_u8(SPLDB_OPCODE_EXPIRETIME_MS);
        buf.put_u64_le(expiry_ms);
    }

    let mut value_buf = BytesMut::new();
    serialize_single_value_data(&mut value_buf, &value.data)?;

    buf.put_u8(value_buf.get_u8());
    write_string(buf, key);
    buf.put(value_buf.freeze());

    Ok(())
}

// --- Length and String Encoding/Decoding Helpers ---

fn write_string(buf: &mut BytesMut, s: &[u8]) {
    write_length_encoding(buf, s.len() as u64);
    buf.put_slice(s);
}

fn read_string(cursor: &mut Bytes) -> io::Result<Bytes> {
    let len = read_length_encoding(cursor)? as usize;
    if cursor.remaining() < len {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Not enough data for string",
        ));
    }
    Ok(cursor.split_to(len))
}

fn write_length_encoding(buf: &mut BytesMut, len: u64) {
    if len < (1 << 6) {
        buf.put_u8(len as u8);
    } else if len < (1 << 14) {
        let val = (len | (1 << 14)) as u16;
        buf.put_u16(val);
    } else if len < (1 << 32) {
        buf.put_u8(0x80);
        buf.put_u32(len as u32);
    } else {
        buf.put_u8(0x81);
        buf.put_u64(len);
    }
}

fn read_length_encoding(cursor: &mut Bytes) -> io::Result<u64> {
    if !cursor.has_remaining() {
        return Err(Error::new(ErrorKind::UnexpectedEof, "Cannot read length"));
    }
    let first_byte = cursor.get_u8();
    match (first_byte & 0xC0) >> 6 {
        0b00 => Ok(u64::from(first_byte & 0x3F)),
        0b01 => {
            if !cursor.has_remaining() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "Cannot read 14-bit length",
                ));
            }
            let next_byte = cursor.get_u8();
            Ok(u64::from(
                ((first_byte as u16 & 0x3F) << 8) | next_byte as u16,
            ))
        }
        0b10 => match first_byte & 0x3F {
            0 => {
                if cursor.remaining() < 4 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Cannot read 32-bit length",
                    ));
                }
                Ok(u64::from(cursor.get_u32()))
            }
            1 => {
                if cursor.remaining() < 8 {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "Cannot read 64-bit length",
                    ));
                }
                Ok(cursor.get_u64())
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Unknown length encoding format",
            )),
        },
        0b11 => Err(Error::new(
            ErrorKind::InvalidData,
            "Special encoded string object not supported as length",
        )),
        _ => unreachable!(),
    }
}

// --- MIGRATE/RESTORE Helpers ---

/// Serializes a single `DataValue` into SPLDB format, used by `MIGRATE`.
pub fn serialize_value(data: &DataValue) -> io::Result<Bytes> {
    let mut buf = BytesMut::new();
    serialize_single_value_data(&mut buf, data)?;
    Ok(buf.freeze())
}

/// Deserializes a single `StoredValue` from SPLDB format, used by `RESTORE`.
pub fn deserialize_value(data: &Bytes) -> io::Result<StoredValue> {
    let mut cursor = data.clone();
    let value_type = cursor.get_u8();
    let data_value = deserialize_single_value_data(&mut cursor, value_type)?;
    Ok(StoredValue::new(data_value))
}

/// Serializes the data part of a `DataValue` (e.g., the list items, hash fields).
fn serialize_single_value_data(buf: &mut BytesMut, data: &DataValue) -> io::Result<()> {
    match data {
        DataValue::String(val) => {
            buf.put_u8(SPLDB_TYPE_STRING);
            write_string(buf, val);
        }
        DataValue::List(list) => {
            buf.put_u8(SPLDB_TYPE_LIST);
            write_length_encoding(buf, list.len() as u64);
            for item in list {
                write_string(buf, item);
            }
        }
        DataValue::Hash(hash) => {
            buf.put_u8(SPLDB_TYPE_HASH);
            write_length_encoding(buf, hash.len() as u64);
            for (field, val) in hash {
                write_string(buf, field);
                write_string(buf, val);
            }
        }
        DataValue::Set(set) => {
            buf.put_u8(SPLDB_TYPE_SET);
            write_length_encoding(buf, set.len() as u64);
            for member in set {
                write_string(buf, member);
            }
        }
        DataValue::SortedSet(zset) => {
            buf.put_u8(SPLDB_TYPE_ZSET);
            write_length_encoding(buf, zset.len() as u64);
            for entry in zset.get_range(0, -1) {
                write_string(buf, &entry.member);
                let mut buffer = ryu::Buffer::new();
                let score_str = buffer.format(entry.score);
                write_string(buf, score_str.as_bytes());
            }
        }
        DataValue::Stream(stream) => {
            buf.put_u8(SPLDB_TYPE_STREAM);
            write_length_encoding(buf, stream.entries.len() as u64);
            for (id, entry) in &stream.entries {
                buf.put_u64_le(id.timestamp_ms);
                buf.put_u64_le(id.sequence);
                write_length_encoding(buf, entry.fields.len() as u64);
                for (field, value) in &entry.fields {
                    write_string(buf, field);
                    write_string(buf, value);
                }
            }
            buf.put_u64_le(stream.length);
            buf.put_u64_le(stream.last_generated_id.timestamp_ms);
            buf.put_u64_le(stream.last_generated_id.sequence);
            write_length_encoding(buf, stream.groups.len() as u64);
            for (group_name, group) in &stream.groups {
                write_string(buf, group_name);
                buf.put_u64_le(group.last_delivered_id.timestamp_ms);
                buf.put_u64_le(group.last_delivered_id.sequence);
                write_length_encoding(buf, group.pending_entries.len() as u64);
                for (id, pel) in &group.pending_entries {
                    buf.put_u64_le(id.timestamp_ms);
                    buf.put_u64_le(id.sequence);
                    write_string(buf, &pel.consumer_name);
                    buf.put_u64_le(pel.delivery_count);
                    buf.put_u64_le(pel.delivery_time_ms);
                }
                write_length_encoding(buf, group.consumers.len() as u64);
                for (consumer_name, consumer) in &group.consumers {
                    write_string(buf, consumer_name);
                    buf.put_u64_le(consumer.seen_time_ms);
                    write_length_encoding(buf, consumer.pending_ids.len() as u64);
                    for id in &consumer.pending_ids {
                        buf.put_u64_le(id.timestamp_ms);
                        buf.put_u64_le(id.sequence);
                    }
                }
            }
        }
        DataValue::Json(val) => {
            buf.put_u8(SPLDB_TYPE_JSON);
            let json_str = serde_json::to_string(val)?;
            write_string(buf, json_str.as_bytes());
        }
        DataValue::HyperLogLog(hll) => {
            buf.put_u8(SPLDB_TYPE_HYPERLOGLOG);
            buf.put_f64(hll.alpha);
            // Serialize all 16384 registers as a sequence of bytes
            buf.extend_from_slice(&hll.registers);
        }
        DataValue::BloomFilter(bf) => {
            buf.put_u8(SPLDB_TYPE_BLOOMFILTER);
            buf.put_u32_le(bf.num_hashes);
            buf.put_u64_le(bf.seeds[0]);
            buf.put_u64_le(bf.seeds[1]);
            write_string(buf, &bf.bits);
        }
        DataValue::HttpCache {
            variants, vary_on, ..
        } => {
            buf.put_u8(SPLDB_TYPE_HTTPCACHE);

            write_length_encoding(buf, vary_on.len() as u64);
            for header in vary_on {
                write_string(buf, header);
            }

            let in_memory_variants: Vec<_> = variants
                .iter()
                .filter(|(_, v)| matches!(v.body, CacheBody::InMemory(_)))
                .collect();

            write_length_encoding(buf, in_memory_variants.len() as u64);
            for (hash, variant) in in_memory_variants {
                buf.put_u64_le(*hash);

                if let CacheBody::InMemory(body_bytes) = &variant.body {
                    write_string(buf, body_bytes);
                } else {
                    unreachable!();
                }

                let mut flags: u8 = 0;
                if variant.metadata.etag.is_some() {
                    flags |= 1 << 0;
                }
                if variant.metadata.last_modified.is_some() {
                    flags |= 1 << 1;
                }
                if variant.metadata.revalidate_url.is_some() {
                    flags |= 1 << 2;
                }
                buf.put_u8(flags);

                if let Some(etag) = &variant.metadata.etag {
                    write_string(buf, etag);
                }
                if let Some(lm) = &variant.metadata.last_modified {
                    write_string(buf, lm);
                }
                if let Some(url) = &variant.metadata.revalidate_url {
                    write_string(buf, url.as_bytes());
                }
            }
        }
    }
    Ok(())
}

/// Deserializes the data part of a `DataValue`.
fn deserialize_single_value_data(cursor: &mut Bytes, value_type: u8) -> io::Result<DataValue> {
    match value_type {
        SPLDB_TYPE_STRING => Ok(DataValue::String(read_string(cursor)?)),
        SPLDB_TYPE_LIST => {
            let len = read_length_encoding(cursor)? as usize;
            let mut list = VecDeque::with_capacity(len);
            for _ in 0..len {
                list.push_back(read_string(cursor)?);
            }
            Ok(DataValue::List(list))
        }
        SPLDB_TYPE_SET => {
            let len = read_length_encoding(cursor)? as usize;
            let mut set = HashSet::with_capacity(len);
            for _ in 0..len {
                set.insert(read_string(cursor)?);
            }
            Ok(DataValue::Set(set))
        }
        SPLDB_TYPE_HASH => {
            let len = read_length_encoding(cursor)? as usize;
            let mut hash = IndexMap::with_capacity(len);
            for _ in 0..len {
                let field = read_string(cursor)?;
                let value = read_string(cursor)?;
                hash.insert(field, value);
            }
            Ok(DataValue::Hash(hash))
        }
        SPLDB_TYPE_ZSET => {
            let len = read_length_encoding(cursor)? as usize;
            let mut zset = SortedSet::new();
            for _ in 0..len {
                let member = read_string(cursor)?;
                let score_bytes = read_string(cursor)?;
                let score_str = std::str::from_utf8(&score_bytes)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                let score: f64 = score_str
                    .parse()
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                zset.add(score, member);
            }
            Ok(DataValue::SortedSet(zset))
        }
        SPLDB_TYPE_STREAM => {
            let mut stream = Stream::new();
            let num_entries = read_length_encoding(cursor)? as usize;
            for _ in 0..num_entries {
                let timestamp_ms = cursor.get_u64_le();
                let sequence = cursor.get_u64_le();
                let id = StreamId::new(timestamp_ms, sequence);
                let num_fields = read_length_encoding(cursor)? as usize;
                let mut fields = IndexMap::with_capacity(num_fields);
                for _ in 0..num_fields {
                    let field = read_string(cursor)?;
                    let value = read_string(cursor)?;
                    fields.insert(field, value);
                }
                stream.entries.insert(id, StreamEntry { id, fields });
            }
            stream.length = cursor.get_u64_le();
            stream.last_generated_id.timestamp_ms = cursor.get_u64_le();
            stream.last_generated_id.sequence = cursor.get_u64_le();
            let num_groups = read_length_encoding(cursor)? as usize;
            for _ in 0..num_groups {
                let group_name = read_string(cursor)?;
                let last_delivered_ts = cursor.get_u64_le();
                let last_delivered_seq = cursor.get_u64_le();
                let mut group = ConsumerGroup {
                    name: group_name.clone(),
                    last_delivered_id: StreamId::new(last_delivered_ts, last_delivered_seq),
                    consumers: HashMap::new(),
                    pending_entries: BTreeMap::new(),
                    idle_index: BTreeSet::new(),
                };
                let num_pending = read_length_encoding(cursor)? as usize;
                for _ in 0..num_pending {
                    let id = StreamId::new(cursor.get_u64_le(), cursor.get_u64_le());
                    let consumer_name = read_string(cursor)?;
                    let delivery_count = cursor.get_u64_le();
                    let delivery_time_ms = cursor.get_u64_le();
                    group.pending_entries.insert(
                        id,
                        PendingEntryInfo {
                            consumer_name,
                            delivery_count,
                            delivery_time_ms,
                        },
                    );
                    group.idle_index.insert((delivery_time_ms, id));
                }
                let num_consumers = read_length_encoding(cursor)? as usize;
                for _ in 0..num_consumers {
                    let consumer_name = read_string(cursor)?;
                    let seen_time_ms = cursor.get_u64_le();
                    let num_pending_for_consumer = read_length_encoding(cursor)? as usize;
                    let mut pending_ids = BTreeSet::new();
                    for _ in 0..num_pending_for_consumer {
                        pending_ids.insert(StreamId::new(cursor.get_u64_le(), cursor.get_u64_le()));
                    }
                    group.consumers.insert(
                        consumer_name.clone(),
                        crate::core::storage::stream::Consumer {
                            name: consumer_name,
                            seen_time_ms,
                            pending_ids,
                        },
                    );
                }
                stream.groups.insert(group_name, group);
            }
            Ok(DataValue::Stream(stream))
        }
        SPLDB_TYPE_JSON => {
            let json_bytes = read_string(cursor)?;
            let value: serde_json::Value = serde_json::from_slice(&json_bytes)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            Ok(DataValue::Json(value))
        }
        SPLDB_TYPE_HYPERLOGLOG => {
            if cursor.remaining() < 8 + 16384 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for HyperLogLog",
                ));
            }
            let alpha = cursor.get_f64();
            let mut registers = [0u8; 16384];
            cursor.copy_to_slice(&mut registers);

            Ok(DataValue::HyperLogLog(Box::new(
                crate::core::storage::hll::HyperLogLog { registers, alpha },
            )))
        }
        SPLDB_TYPE_BLOOMFILTER => {
            if cursor.remaining() < 4 + 8 + 8 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for BloomFilter metadata",
                ));
            }
            let num_hashes = cursor.get_u32_le();
            let seed1 = cursor.get_u64_le();
            let seed2 = cursor.get_u64_le();
            let bits = read_string(cursor)?.to_vec();
            Ok(DataValue::BloomFilter(Box::new(
                crate::core::storage::bloom::BloomFilter {
                    bits,
                    num_hashes,
                    seeds: [seed1, seed2],
                },
            )))
        }
        SPLDB_TYPE_HTTPCACHE => {
            let vary_len = read_length_encoding(cursor)? as usize;
            let mut vary_on = Vec::with_capacity(vary_len);
            for _ in 0..vary_len {
                vary_on.push(read_string(cursor)?);
            }

            let variants_len = read_length_encoding(cursor)? as usize;
            let mut variants = HashMap::with_capacity(variants_len);
            for _ in 0..variants_len {
                let hash = cursor.get_u64_le();
                let body = read_string(cursor)?;

                let flags = cursor.get_u8();
                let mut metadata = HttpMetadata::default();
                if (flags & (1 << 0)) != 0 {
                    metadata.etag = Some(read_string(cursor)?);
                }
                if (flags & (1 << 1)) != 0 {
                    metadata.last_modified = Some(read_string(cursor)?);
                }
                if (flags & (1 << 2)) != 0 {
                    metadata.revalidate_url =
                        Some(String::from_utf8_lossy(&read_string(cursor)?).to_string());
                }

                variants.insert(
                    hash,
                    CacheVariant {
                        body: CacheBody::InMemory(body),
                        metadata,
                        last_accessed: Instant::now(),
                    },
                );
            }
            Ok(DataValue::HttpCache {
                variants,
                vary_on,
                tags_epoch: 0,
            })
        }
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("Unknown SPLDB value type: {other:#04x}"),
        )),
    }
}
