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
    /// The cache body is stored in memory.
    InMemory(Bytes),
    /// The cache body is stored on disk at the specified path.
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
    /// The ETag header value from the origin response.
    pub etag: Option<Bytes>,
    /// The Last-Modified header value from the origin response.
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
    /// The body of the cached response.
    pub body: CacheBody,
    /// The HTTP metadata associated with this variant.
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
    /// A unique name for the policy.
    pub name: String,
    /// A glob pattern that matches cache keys this policy applies to.
    pub key_pattern: String,
    /// A URL template for fetching content from the origin.
    pub url_template: String,
    /// The time-to-live in seconds for fresh content.
    pub ttl: Option<u64>,
    /// The stale-while-revalidate period in seconds.
    pub swr: Option<u64>,
    /// The grace period in seconds to serve stale content if the origin is down.
    pub grace: Option<u64>,
    /// A list of static or dynamic tags associated with items cached under this policy.
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
    /// A list of header names to use for the Vary mechanism.
    /// CACHE.PROXY will automatically use these headers to create cache variants.
    #[serde(default)]
    pub vary_on: Vec<String>,
    /// If true, the server will try to parse Cache-Control headers from the origin.
    #[serde(default)]
    pub respect_origin_headers: bool,
    /// A policy-specific TTL for negative caching.
    pub negative_ttl: Option<u64>,
}

/// The persistent state of an on-disk cache file, logged in the manifest.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ManifestState {
    /// The file is being written to disk.
    Pending,
    /// The file has been successfully written and is associated with a cache key.
    Committed,
    /// The associated cache key has been deleted; the file is scheduled for garbage collection.
    PendingDelete,
}

/// A single entry in the on-disk cache manifest file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// The UNIX timestamp when the entry was logged.
    pub timestamp: u64,
    /// The current state of the file.
    pub state: ManifestState,
    /// The path to the on-disk cache file.
    pub path: PathBuf,
    /// The key associated with this file, used for eviction.
    pub key: Bytes,
}
