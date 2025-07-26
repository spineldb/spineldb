// src/core/commands/cache/helpers.rs

//! Contains shared helper functions for the CACHE.* command family.

use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Normalizes common HTTP header values before hashing.
///
/// This improves the cache hit ratio by treating semantically identical but
/// syntactically different header values as the same variant.
fn normalize_header_value(header_name: &Bytes, header_value: &Bytes) -> Bytes {
    if header_name.eq_ignore_ascii_case(b"accept-language") {
        // Normalizes language tags by ignoring q-factors and case.
        // e.g., "en-US,en;q=0.9, fr;q=0.8" -> "en-us,en,fr"
        let value_str = String::from_utf8_lossy(header_value);
        let normalized: String = value_str
            .split(',')
            .filter_map(|part| part.split(';').next())
            .map(|lang| lang.trim().to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(",");
        return Bytes::from(normalized);
    }

    if header_name.eq_ignore_ascii_case(b"accept-encoding") {
        // Normalizes encoding by ignoring order and q-factors.
        // e.g., "gzip, deflate, br" becomes the same hash as "br, gzip, deflate"
        let value_str = String::from_utf8_lossy(header_value);
        let mut encodings: Vec<&str> = value_str
            .split(',')
            .map(|s| s.trim().split(';').next().unwrap_or("").trim())
            .collect();
        encodings.sort_unstable();
        return Bytes::from(encodings.join(","));
    }

    // Default: return the original value if no specific normalization rule applies.
    header_value.clone()
}

/// Calculates a variant hash based on a list of `Vary` headers and the provided request headers.
///
/// This function ensures consistent hashing by sorting the relevant headers before hashing their values.
pub fn calculate_variant_hash(vary_on: &[Bytes], headers: &Option<Vec<(Bytes, Bytes)>>) -> u64 {
    let mut hasher = DefaultHasher::new();
    if vary_on.is_empty() {
        return 0; // No vary headers means only one variant is possible.
    }

    if let Some(headers) = headers {
        let mut sorted_headers = headers.clone();
        // Sort headers to ensure consistent hashing regardless of client order.
        sorted_headers.sort_by(|a, b| a.0.cmp(&b.0));
        for (k, v) in sorted_headers {
            if vary_on.iter().any(|h| h.eq_ignore_ascii_case(&k)) {
                // Normalize the value before hashing for better hit ratio.
                let normalized_value = normalize_header_value(&k, &v);
                normalized_value.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}
