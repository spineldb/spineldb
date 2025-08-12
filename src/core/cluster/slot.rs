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
