// src/core/storage/cache_types.rs

//! Defines data structures specific to the Intelligent Cache feature.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

/// Represents the location of a cache body, either in RAM or on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheBody {
    InMemory(Bytes),
    OnDisk { path: PathBuf, size: u64 },
}

impl CacheBody {
    /// Returns the size of the cache body in bytes.
    pub fn len(&self) -> usize {
        match self {
            CacheBody::InMemory(b) => b.len(),
            CacheBody::OnDisk { size, .. } => *size as usize,
        }
    }

    /// Returns `true` if the cache body has a length of zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Stores HTTP-related metadata alongside a cached response body.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpMetadata {
    pub etag: Option<Bytes>,
    pub last_modified: Option<Bytes>,
    /// The URL used to fetch/revalidate this content, essential for proactive revalidation.
    pub revalidate_url: Option<String>,
}

impl HttpMetadata {
    /// Calculates the memory usage of the metadata fields themselves.
    pub fn memory_usage(&self) -> usize {
        let etag_size = self.etag.as_ref().map_or(0, |b| b.len());
        let lm_size = self.last_modified.as_ref().map_or(0, |b| b.len());
        let url_size = self.revalidate_url.as_ref().map_or(0, |s| s.len());
        etag_size + lm_size + url_size
    }
}

/// Represents a single version of a cached object, determined by Vary headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheVariant {
    pub body: CacheBody,
    pub metadata: HttpMetadata,
    /// Tracks the last time this specific variant was requested.
    /// This is a runtime metric and is not persisted.
    pub last_accessed: Instant,
}

/// A map from a variant hash to the actual cached variant data.
/// The hash is generated from the values of the headers specified in `Vary`.
pub type VariantMap = HashMap<u64, CacheVariant>;

/// Represents a declarative caching rule. These are defined by the user
/// and stored in the server state to automate caching behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachePolicy {
    pub name: String,
    pub key_pattern: String,
    pub url_template: String,
    pub ttl: Option<u64>,
    pub swr: Option<u64>,
    pub grace: Option<u64>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// If true, the revalidator task will proactively try to keep items
    /// matching this policy fresh, even before they are requested.
    #[serde(default)]
    pub prewarm: bool,
    /// A list of HTTP status codes from the origin that should NOT be cached.
    #[serde(default)]
    pub disallow_status_codes: Vec<u16>,
    /// The maximum size in bytes for an object to be cached under this policy.
    pub max_size_bytes: Option<u64>,
}

/// The persistent state of an on-disk cache file, logged in the manifest.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ManifestState {
    Pending,
    Committed,
    PendingDelete,
}

/// A single entry in the on-disk cache manifest file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub timestamp: u64,
    pub state: ManifestState,
    pub path: PathBuf,
}
