// src/core/storage/bloom.rs

use bytes::Bytes;
use murmur3::murmur3_x64_128;
use std::io::Cursor;

/// A Bloom filter implementation for probabilistic set membership testing.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    pub bits: Vec<u8>,
    pub num_hashes: u32,
    pub seeds: [u64; 2], // Two seeds for double hashing
    pub capacity: u64,
    pub error_rate: f64,
    pub items_added: u64,
}

impl BloomFilter {
    const BF_MAGIC: &'static [u8] = b"SPINELBF";
    const BF_ENCODING_VERSION: u8 = 2;

    /// Creates a new Bloom filter with optimal parameters.
    ///
    /// # Arguments
    /// * `capacity` - The expected number of items to be inserted.
    /// * `error_rate` - The desired false positive probability (e.g., 0.01 for 1%).
    pub fn new(capacity: u64, error_rate: f64) -> Self {
        let m = Self::optimal_m(capacity, error_rate);
        let k = Self::optimal_k(capacity, m);
        Self {
            bits: vec![0; m as usize],
            num_hashes: k,
            seeds: [rand::random::<u64>(), rand::random::<u64>()],
            capacity,
            error_rate,
            items_added: 0,
        }
    }

    /// Calculates the optimal number of bits (m).
    fn optimal_m(capacity: u64, error_rate: f64) -> u64 {
        let m_bits = -((capacity as f64 * error_rate.ln()) / (2.0_f64.ln().powi(2)));
        // Return number of bytes, rounding up.
        (m_bits.ceil() as u64).div_ceil(8)
    }

    /// Calculates the optimal number of hash functions (k).
    fn optimal_k(capacity: u64, m: u64) -> u32 {
        let k = ((m as f64 / capacity as f64) * 2.0_f64.ln()).round() as u32;
        k.max(1)
    }

    /// Hashes an item to get two initial hash values.
    fn hash_core(&self, item: &Bytes) -> (u64, u64) {
        let hash128 = murmur3_x64_128(&mut Cursor::new(item), self.seeds[0] as u32).unwrap();
        let h1 = hash128 as u64;
        let h2 = (hash128 >> 64) as u64;
        (h1, h2)
    }

    /// Adds an item to the filter. Returns true if a bit was flipped, false otherwise.
    pub fn add(&mut self, item: &Bytes) -> bool {
        let (h1, h2) = self.hash_core(item);
        let mut changed = false;
        for i in 0..self.num_hashes {
            let index =
                (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (self.bits.len() as u64 * 8);
            let byte_index = (index / 8) as usize;
            let bit_index = (index % 8) as u8;
            if (self.bits[byte_index] & (1 << bit_index)) == 0 {
                self.bits[byte_index] |= 1 << bit_index;
                changed = true;
            }
        }
        if changed {
            self.items_added += 1;
        }
        changed
    }

    /// Checks if an item is possibly in the set.
    /// Returns false if the item is definitely not in the set.
    /// Returns true if the item is *probably* in the set.
    pub fn check(&self, item: &Bytes) -> bool {
        let (h1, h2) = self.hash_core(item);
        for i in 0..self.num_hashes {
            let index =
                (h1.wrapping_add((i as u64).wrapping_mul(h2))) % (self.bits.len() as u64 * 8);
            let byte_index = (index / 8) as usize;
            let bit_index = (index % 8) as u8;
            if (self.bits[byte_index] & (1 << bit_index)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.bits.capacity()
    }

    /// Serializes the Bloom Filter to a compact binary format.
    /// V2 Format: "SPINELBF" (8) | version (1) | num_hashes (4) | seed1 (8) | seed2 (8) | capacity (8) | error_rate (8) | items_added (8) | bits
    pub fn serialize(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(8 + 1 + 4 + 8 + 8 + 8 + 8 + 8 + self.bits.len());
        bytes.extend_from_slice(Self::BF_MAGIC);
        bytes.push(Self::BF_ENCODING_VERSION);
        bytes.extend_from_slice(&self.num_hashes.to_le_bytes());
        bytes.extend_from_slice(&self.seeds[0].to_le_bytes());
        bytes.extend_from_slice(&self.seeds[1].to_le_bytes());
        bytes.extend_from_slice(&self.capacity.to_le_bytes());
        bytes.extend_from_slice(&self.error_rate.to_le_bytes());
        bytes.extend_from_slice(&self.items_added.to_le_bytes());
        bytes.extend_from_slice(&self.bits);
        Bytes::from(bytes)
    }

    /// Deserializes a Bloom Filter from the binary format.
    pub fn deserialize(data: &Bytes) -> Option<Self> {
        if !data.starts_with(Self::BF_MAGIC) {
            return None;
        }
        let mut cursor = 8;
        let version = *data.get(cursor)?;
        cursor += 1;

        if version > Self::BF_ENCODING_VERSION {
            return None; // Do not support future versions
        }

        let num_hashes = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;

        let seed1 = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
        cursor += 8;

        let seed2 = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
        cursor += 8;

        let (capacity, error_rate, items_added, bits) = if version == 1 {
            let bits = data.get(cursor..)?.to_vec();
            // V1 did not store this info, so we use 0 as a placeholder.
            (0, 0.0, 0, bits)
        } else {
            // Version 2 or higher
            let capacity = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
            cursor += 8;
            let error_rate = f64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
            cursor += 8;
            let items_added = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
            cursor += 8;
            let bits = data.get(cursor..)?.to_vec();
            (capacity, error_rate, items_added, bits)
        };

        Some(Self {
            bits,
            num_hashes,
            seeds: [seed1, seed2],
            capacity,
            error_rate,
            items_added,
        })
    }
}
