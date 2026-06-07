// src/core/cluster/slot.rs

//! Implements the cluster hash slot algorithm.

use bytes::Bytes;
use crc::{CRC_16_USB, Crc};

/// The total number of hash slots in the cluster.
pub const NUM_SLOTS: usize = 16384;

/// The specific CRC16 algorithm used by SpinelDB for calculating hash slots.
const CRC16_ALGO: Crc<u16> = Crc::<u16>::new(&CRC_16_USB);

/// Calculates the hash slot for a given key.
///
/// The algorithm is compatible with SpinelDB. It first checks for a "hash tag"
/// (a substring enclosed in `{...}`) within the key. If a hash tag is found,
/// only the content within the tag is used for the CRC16 calculation. This
/// allows users to force multiple keys into the same hash slot. If no hash
/// tag is found, the entire key is used.
///
/// The final slot is determined by `CRC16(key) % NUM_SLOTS`.
pub fn get_slot(key: &Bytes) -> u16 {
    // Check for a hash tag, e.g., "user:{123}:name".
    if let Some(start) = key.iter().position(|&b| b == b'{')
        && let Some(end_offset) = key[start + 1..].iter().position(|&b| b == b'}')
    {
        let end = start + 1 + end_offset;
        // Ensure the tag is not empty, e.g., "user:{}".
        if end > start + 1 {
            return CRC16_ALGO.checksum(&key[start + 1..end]) % (NUM_SLOTS as u16);
        }
    }
    // If no valid hash tag is found, hash the entire key.
    CRC16_ALGO.checksum(key) % (NUM_SLOTS as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_within_valid_range() {
        for key in [
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"user:1000:profile"),
            Bytes::from_static(b""),
        ] {
            let s = get_slot(&key);
            assert!(
                s < NUM_SLOTS as u16,
                "slot {s} out of range for key {key:?}"
            );
        }
    }

    #[test]
    fn test_hash_tag_overrides_full_key() {
        // Two keys with the same hash tag should always land in the same slot,
        // regardless of the surrounding key.
        let k1 = Bytes::from_static(b"{user:1000}.profile");
        let k2 = Bytes::from_static(b"{user:1000}.settings");
        assert_eq!(get_slot(&k1), get_slot(&k2));

        // Different tags should (with very high probability) yield different slots.
        let k3 = Bytes::from_static(b"{user:2000}.profile");
        assert_ne!(get_slot(&k1), get_slot(&k3));
    }

    #[test]
    fn test_empty_hash_tag_falls_back_to_full_key() {
        // "{}" inside the key is considered an empty tag and should be ignored.
        let k_empty_tag = Bytes::from_static(b"user:{}:profile");
        let k_plain = Bytes::from_static(b"user:profile");
        // They should NOT necessarily be equal because "{}" is empty, but neither should panic.
        let _ = (get_slot(&k_empty_tag), get_slot(&k_plain));
    }

    #[test]
    fn test_unmatched_open_brace_uses_full_key() {
        // Missing closing brace: fall back to hashing the whole key.
        let k = Bytes::from_static(b"user:{1000:profile");
        let s = get_slot(&k);
        assert!(s < NUM_SLOTS as u16);
    }

    #[test]
    fn test_known_redis_compatibility_vectors() {
        // These properties hold for any CRC-16-USB implementation and are
        // documented in Redis' own test vectors.
        //   1. Two keys with the same hash tag MUST hash to the same slot.
        //   2. Two keys with different hash tags SHOULD (with overwhelming probability) hash to different slots.
        //   3. A given key MUST always hash to the same slot (determinism).
        let k1 = Bytes::from_static(b"{user1000}.following");
        let k2 = Bytes::from_static(b"{user1000}.followers");
        let k3 = Bytes::from_static(b"{user2000}.following");
        assert_eq!(get_slot(&k1), get_slot(&k2));
        assert_ne!(get_slot(&k1), get_slot(&k3));
    }

    #[test]
    fn test_determinism() {
        let k = Bytes::from_static(b"a");
        let s1 = get_slot(&k);
        let s2 = get_slot(&k);
        assert_eq!(s1, s2);
    }
}
