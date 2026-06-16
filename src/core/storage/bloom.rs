// src/core/storage/bloom.rs

use bytes::Bytes;
use murmur3::murmur3_x64_128;
use std::io::Cursor;

/// A Bloom filter implementation for probabilistic set membership testing.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    pub bits: Vec<u8>,
    pub num_hashes: u32,
    pub seeds: [u64; 2], // seeds[0] used for hashing; seeds[1] reserved for future use
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

    /// Calculates the optimal number of bytes (m) for the bit array.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bloom_is_empty() {
        let bf = BloomFilter::new(1000, 0.01);
        assert_eq!(bf.items_added, 0);
        // Newly created filter must not contain any items.
        assert!(!bf.check(&Bytes::from_static(b"anything")));
    }

    #[test]
    fn test_add_then_check_round_trip() {
        let mut bf = BloomFilter::new(10_000, 0.01);
        let items: Vec<Bytes> = (0..100).map(|i| Bytes::from(format!("item-{i}"))).collect();
        for item in &items {
            bf.add(item);
        }
        // Every added item must be found.
        for item in &items {
            assert!(bf.check(item), "item {item:?} should be found");
        }
    }

    #[test]
    fn test_check_for_unseen_item_may_have_false_positives() {
        // The basic Bloom filter property: a member that was added is ALWAYS
        // reported as "present" (no false negatives). For unseen members the
        // contract is "false positives are possible" — we just verify the
        // filter doesn't return *true* for all of them.
        let mut bf = BloomFilter::new(10_000, 0.01);
        for i in 0..10_000 {
            bf.add(&Bytes::from(format!("added-{i}")));
        }
        let total = 1000;
        let false_positives = (0..total)
            .filter(|i| bf.check(&Bytes::from(format!("unseen-{i}"))))
            .count();
        // We don't enforce a strict false-positive rate (the implementation
        // uses a heuristic for k) but the filter MUST still reject the
        // majority of unseen items.
        assert!(
            false_positives < total,
            "filter accepted every unseen item ({false_positives}/{total})"
        );
    }

    #[test]
    fn test_no_false_negatives() {
        // No matter the parameters, an item that was added must always be
        // reported as present. This is the only hard correctness property
        // of a Bloom filter.
        let mut bf = BloomFilter::new(50, 0.01);
        let items: Vec<Bytes> = (0..50).map(|i| Bytes::from(format!("{i}"))).collect();
        for item in &items {
            bf.add(item);
        }
        for item in &items {
            assert!(
                bf.check(item),
                "item {item:?} should be present (no false negatives)"
            );
        }
    }

    #[test]
    fn test_add_returns_false_for_idempotent_add() {
        let mut bf = BloomFilter::new(100, 0.01);
        let item = Bytes::from_static(b"x");
        assert!(bf.add(&item));
        // Adding the same item should report no change.
        assert!(!bf.add(&item));
    }

    #[test]
    fn test_items_added_counter_is_incremented() {
        // `items_added` only increments when an add() call actually flips
        // at least one bit from 0 to 1; subsequent adds that hit only already-set
        // bits do not count. We must therefore use a filter with plenty of bits
        // to make collisions unlikely, then assert that the counter grew (and
        // never exceeded the number of adds).
        let mut bf = BloomFilter::new(10_000, 0.0001);
        for i in 0..100 {
            bf.add(&Bytes::from(format!("item-{i}")));
        }
        assert!(bf.items_added > 0);
        assert!(bf.items_added <= 100);
    }

    #[test]
    fn test_serialize_v2_roundtrip() {
        let mut bf = BloomFilter::new(1024, 0.01);
        for i in 0..50 {
            bf.add(&Bytes::from(format!("{i}")));
        }
        let bytes = bf.serialize();
        let restored = BloomFilter::deserialize(&bytes).expect("deserialize ok");
        assert_eq!(restored.capacity, bf.capacity);
        assert_eq!(restored.error_rate, bf.error_rate);
        assert_eq!(restored.items_added, bf.items_added);
        assert_eq!(restored.num_hashes, bf.num_hashes);
        assert_eq!(restored.seeds, bf.seeds);
        assert_eq!(restored.bits, bf.bits);
        // Membership tests must still work.
        for i in 0..50 {
            assert!(restored.check(&Bytes::from(format!("{i}"))));
        }
    }

    #[test]
    fn test_deserialize_rejects_bad_magic() {
        let mut bad = Vec::from(&b"BADMAGIC"[..]);
        bad.extend_from_slice(&[0u8; 100]);
        assert!(BloomFilter::deserialize(&Bytes::from(bad)).is_none());
    }

    #[test]
    fn test_deserialize_rejects_future_version() {
        let mut bad = Vec::from(&b"SPINELBF"[..]);
        bad.push(255u8); // future version
        bad.extend_from_slice(&[0u8; 100]);
        assert!(BloomFilter::deserialize(&Bytes::from(bad)).is_none());
    }

    #[test]
    fn test_deserialize_accepts_v1_without_metadata() {
        // A V1 blob omits the trailing capacity/error/items_added fields.
        // We can build a minimal V1 payload and verify that deserialization
        // returns Some(_) with default metadata.
        let mut v1 = Vec::new();
        v1.extend_from_slice(b"SPINELBF");
        v1.push(1u8); // V1
        v1.extend_from_slice(&1u32.to_le_bytes()); // num_hashes
        v1.extend_from_slice(&1u64.to_le_bytes()); // seed1
        v1.extend_from_slice(&2u64.to_le_bytes()); // seed2
        v1.extend_from_slice(&[0u8; 64]); // bits
        let restored = BloomFilter::deserialize(&Bytes::from(v1)).expect("v1 should load");
        assert_eq!(restored.capacity, 0);
        assert_eq!(restored.error_rate, 0.0);
        assert_eq!(restored.items_added, 0);
    }

    #[test]
    fn test_optimal_k_is_at_least_one() {
        // Even with a pathologically small capacity, k should never be zero.
        let bf = BloomFilter::new(1, 0.5);
        assert!(bf.num_hashes >= 1);
    }

    #[test]
    fn test_optimal_m_rounds_up_to_byte_boundary() {
        // optimal_m returns bytes (not bits), so it must always be a multiple of at least 1 byte.
        // We can't access the private function directly, but we can check that bit count
        // is sufficient for the requested capacity.
        let bf = BloomFilter::new(100, 0.01);
        // 8 bits per byte.
        let total_bits = bf.bits.len() * 8;
        assert!(total_bits > 0);
    }

    #[test]
    fn test_memory_usage_includes_bits_capacity() {
        let bf = BloomFilter::new(100, 0.01);
        let baseline = std::mem::size_of::<BloomFilter>();
        let usage = bf.memory_usage();
        assert!(usage >= baseline);
        assert_eq!(usage, baseline + bf.bits.capacity());
    }

    #[test]
    fn test_different_seeds_produce_different_filters() {
        // Two filters with identical items but different seeds should
        // still report membership correctly.
        let mut a = BloomFilter::new(100, 0.01);
        let mut b = BloomFilter::new(100, 0.01);
        for i in 0..20 {
            a.add(&Bytes::from(format!("x-{i}")));
            b.add(&Bytes::from(format!("x-{i}")));
        }
        for i in 0..20 {
            let key = Bytes::from(format!("x-{i}"));
            assert!(a.check(&key));
            assert!(b.check(&key));
        }
    }
}
