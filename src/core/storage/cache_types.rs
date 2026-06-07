// src/core/storage/cache_types.rs

//! Defines data structures specific to the Intelligent Cache feature.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

/// Represents the location and state of a cache body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheBody {
    /// The cache body is stored in memory.
    InMemory(Bytes),
    /// The cache body is stored on disk at the specified path.
    OnDisk { path: PathBuf, size: u64 },
    /// Represents a negatively cached response (e.g., from a 404 or 500 origin error).
    Negative { status: u16, body: Option<Bytes> },
    /// The cache body is stored compressed (zstd) in memory.
    CompressedInMemory { original_size: usize, data: Bytes },
}

impl CacheBody {
    /// Returns the original, uncompressed size of the cache body in bytes.
    pub fn len(&self) -> usize {
        match self {
            CacheBody::InMemory(b) => b.len(),
            CacheBody::OnDisk { size, .. } => *size as usize,
            CacheBody::Negative { body, .. } => body.as_ref().map_or(0, |b| b.len()),
            CacheBody::CompressedInMemory { original_size, .. } => *original_size,
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
    /// The content encoding used if the body is compressed (e.g., "zstd").
    pub content_encoding: Option<Bytes>,
}

impl HttpMetadata {
    /// Calculates the memory usage of the metadata fields themselves.
    pub fn memory_usage(&self) -> usize {
        let etag_size = self.etag.as_ref().map_or(0, |b| b.len());
        let lm_size = self.last_modified.as_ref().map_or(0, |b| b.len());
        let url_size = self.revalidate_url.as_ref().map_or(0, |s| s.len());
        let encoding_size = self.content_encoding.as_ref().map_or(0, |b| b.len());
        etag_size + lm_size + url_size + encoding_size
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
    /// A URL template for fetching content from the origin. Can contain placeholders
    /// from the key pattern (e.g., `{1}`) or request headers (e.g., `{hdr:Header-Name}`).
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
    /// The priority of the policy (higher numbers are checked first) when a key matches multiple policies.
    #[serde(default)]
    pub priority: u8,
    /// If true, enables transparent compression (zstd) for items cached under this policy.
    #[serde(default)]
    pub compression: bool,
    /// If true, forces items to be stored on disk, even if smaller than the streaming threshold.
    #[serde(default)]
    pub force_disk: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_body_in_memory_len() {
        let b = CacheBody::InMemory(Bytes::from_static(b"hello"));
        assert_eq!(b.len(), 5);
        assert!(!b.is_empty());
    }

    #[test]
    fn test_cache_body_in_memory_empty() {
        let b = CacheBody::InMemory(Bytes::new());
        assert_eq!(b.len(), 0);
        assert!(b.is_empty());
    }

    #[test]
    fn test_cache_body_on_disk_len() {
        let b = CacheBody::OnDisk {
            path: PathBuf::from("/tmp/cache/x"),
            size: 1024,
        };
        assert_eq!(b.len(), 1024);
        assert!(!b.is_empty());
    }

    #[test]
    fn test_cache_body_negative_with_body() {
        let b = CacheBody::Negative {
            status: 404,
            body: Some(Bytes::from_static(b"Not Found")),
        };
        assert_eq!(b.len(), 9);
    }

    #[test]
    fn test_cache_body_negative_without_body() {
        let b = CacheBody::Negative {
            status: 500,
            body: None,
        };
        assert_eq!(b.len(), 0);
        assert!(b.is_empty());
    }

    #[test]
    fn test_cache_body_compressed_len_uses_original_size() {
        let b = CacheBody::CompressedInMemory {
            original_size: 2000,
            data: Bytes::from_static(b"zstd"), // compressed payload is tiny
        };
        // len() reports the original (uncompressed) size, not the on-wire size.
        assert_eq!(b.len(), 2000);
    }

    #[test]
    fn test_http_metadata_default_is_empty() {
        let m = HttpMetadata::default();
        assert!(m.etag.is_none());
        assert!(m.last_modified.is_none());
        assert!(m.revalidate_url.is_none());
        assert!(m.content_encoding.is_none());
        assert_eq!(m.memory_usage(), 0);
    }

    #[test]
    fn test_http_metadata_memory_usage() {
        let m = HttpMetadata {
            etag: Some(Bytes::from_static(b"\"abc\"")),
            last_modified: Some(Bytes::from_static(b"Today")),
            revalidate_url: Some("https://x/y".to_string()),
            content_encoding: Some(Bytes::from_static(b"zstd")),
        };
        // 5 ("abc") + 5 (Today) + 11 (https://x/y) + 4 (zstd) = 25
        assert_eq!(m.memory_usage(), 25);
    }

    #[test]
    fn test_cache_variant_equality() {
        let v1 = CacheVariant {
            body: CacheBody::InMemory(Bytes::from_static(b"x")),
            metadata: HttpMetadata::default(),
            last_accessed: Instant::now(),
        };
        let v2 = v1.clone();
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_cache_policy_defaults() {
        let p = CachePolicy {
            name: "p".to_string(),
            key_pattern: "k:*".to_string(),
            url_template: "u".to_string(),
            ttl: None,
            swr: None,
            grace: None,
            tags: vec![],
            prewarm: false,
            disallow_status_codes: vec![],
            max_size_bytes: None,
            vary_on: vec![],
            respect_origin_headers: false,
            negative_ttl: None,
            priority: 0,
            compression: false,
            force_disk: false,
        };
        assert_eq!(p.name, "p");
        assert!(p.tags.is_empty());
        assert!(!p.prewarm);
        assert_eq!(p.priority, 0);
    }

    #[test]
    fn test_manifest_state_eq() {
        assert_eq!(ManifestState::Pending, ManifestState::Pending);
        assert_ne!(ManifestState::Pending, ManifestState::Committed);
        assert_ne!(ManifestState::Committed, ManifestState::PendingDelete);
    }

    #[test]
    fn test_manifest_entry_construction() {
        let e = ManifestEntry {
            timestamp: 1700000000,
            state: ManifestState::Committed,
            path: PathBuf::from("/var/cache/k.bin"),
            key: Bytes::from_static(b"k"),
        };
        assert_eq!(e.timestamp, 1700000000);
        assert_eq!(e.state, ManifestState::Committed);
        assert_eq!(e.key, Bytes::from_static(b"k"));
    }
}
