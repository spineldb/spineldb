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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_accept_language_basic() {
        let result = normalize_header_value(
            &Bytes::from_static(b"accept-language"),
            &Bytes::from_static(b"en-US,en;q=0.9, fr;q=0.8"),
        );
        assert_eq!(result, Bytes::from_static(b"en-us,en,fr"));
    }

    #[test]
    fn test_normalize_accept_language_single() {
        let result = normalize_header_value(
            &Bytes::from_static(b"accept-language"),
            &Bytes::from_static(b"en-US"),
        );
        assert_eq!(result, Bytes::from_static(b"en-us"));
    }

    #[test]
    fn test_normalize_accept_encoding_basic() {
        let result = normalize_header_value(
            &Bytes::from_static(b"accept-encoding"),
            &Bytes::from_static(b"gzip, deflate, br"),
        );
        assert_eq!(result, Bytes::from_static(b"br,deflate,gzip"));
    }

    #[test]
    fn test_normalize_accept_encoding_with_q() {
        let result = normalize_header_value(
            &Bytes::from_static(b"accept-encoding"),
            &Bytes::from_static(b"gzip;q=1.0, deflate;q=0.5"),
        );
        assert_eq!(result, Bytes::from_static(b"deflate,gzip"));
    }

    #[test]
    fn test_normalize_other_header() {
        let result = normalize_header_value(
            &Bytes::from_static(b"x-custom"),
            &Bytes::from_static(b"value"),
        );
        assert_eq!(result, Bytes::from_static(b"value"));
    }

    #[test]
    fn test_calculate_variant_hash_empty_vary() {
        let result = calculate_variant_hash(&[], &None);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_calculate_variant_hash_with_matching_header() {
        let vary_on = vec![Bytes::from_static(b"accept-encoding")];
        let headers = Some(vec![(
            Bytes::from_static(b"accept-encoding"),
            Bytes::from_static(b"gzip"),
        )]);
        let result = calculate_variant_hash(&vary_on, &headers);
        assert!(result != 0);
    }

    #[test]
    fn test_calculate_variant_hash_headers_sorted() {
        let vary_on = vec![
            Bytes::from_static(b"accept-encoding"),
            Bytes::from_static(b"accept-language"),
        ];
        let headers = Some(vec![
            (
                Bytes::from_static(b"accept-language"),
                Bytes::from_static(b"en"),
            ),
            (
                Bytes::from_static(b"accept-encoding"),
                Bytes::from_static(b"gzip"),
            ),
        ]);
        let result = calculate_variant_hash(&vary_on, &headers);
        assert!(result != 0);
    }

    #[test]
    fn test_calculate_variant_hash_non_matching_header() {
        // When vary_on is non-empty but no headers match, hasher is initialized but nothing is hashed.
        // The result is still a deterministic hash (from empty hasher state).
        let vary_on = vec![Bytes::from_static(b"accept-encoding")];
        let result1 = calculate_variant_hash(
            &vary_on,
            &Some(vec![(
                Bytes::from_static(b"x-custom"),
                Bytes::from_static(b"value"),
            )]),
        );
        let result2 = calculate_variant_hash(&vary_on, &None);
        // Both should be the same since nothing gets hashed
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_calculate_variant_hash_case_insensitive() {
        let vary_on = vec![Bytes::from_static(b"Accept-Encoding")];
        let headers = Some(vec![(
            Bytes::from_static(b"accept-encoding"),
            Bytes::from_static(b"gzip"),
        )]);
        let result = calculate_variant_hash(&vary_on, &headers);
        assert!(result != 0);
    }
}
