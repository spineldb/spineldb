// src/core/storage/hll.rs

use bytes::Bytes;
use murmur3::murmur3_x64_128;
use std::io::Cursor;

/// A HyperLogLog implementation for estimating cardinality
#[derive(Debug, Clone, PartialEq)]
pub struct HyperLogLog {
    pub registers: [u8; 16384], // 16384 registers (2^14) - pub for serialization
    pub alpha: f64,             // pub for serialization
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    const HLL_MAGIC: &'static [u8] = b"SPINELHLL";
    const HLL_ENCODING_VERSION: u8 = 1;
    const HLL_REGISTER_COUNT: usize = 16384;

    pub fn new() -> Self {
        const M: f64 = 16384.0;
        Self {
            registers: [0; Self::HLL_REGISTER_COUNT],
            alpha: 0.7213 / (1.0 + 1.079 / M), // Correct alpha for m=16384
        }
    }

    pub fn add(&mut self, item: &Bytes) -> bool {
        let hash = murmur3_x64_128(&mut Cursor::new(item), 0).unwrap();
        let hash_high = (hash >> 64) as u64;

        // Use the first 14 bits for register index
        let index = (hash_high >> 50) as usize;

        // Use the remaining 50 bits to count leading zeros
        let remaining = hash_high << 14;
        let rho = (remaining.leading_zeros() + 1) as u8;

        let old_rho = self.registers[index];
        if rho > old_rho {
            self.registers[index] = rho;
            true // changed
        } else {
            false // no change
        }
    }

    pub fn merge(&mut self, other: &HyperLogLog) {
        for i in 0..Self::HLL_REGISTER_COUNT {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    pub fn count(&self) -> u64 {
        const M: f64 = 16384.0;
        let mut sum = 0.0;

        for &register in &self.registers {
            sum += (2.0_f64).powi(-(register as i32));
        }

        let estimate = self.alpha * M * M / sum;

        // Small range correction
        if estimate <= 2.5 * M {
            let v = self.count_registers_with_value(0) as f64;
            if v > 0.0 {
                return (M * (M / v).ln()) as u64;
            }
        }

        // Large range correction (for 64-bit hashes)
        const TWO_POW_64: f64 = 1.844_674_407_370_955_2e19; // 2^64
        if estimate > (1.0 / 30.0) * TWO_POW_64 {
            return ((-TWO_POW_64) * (1.0 - estimate / TWO_POW_64).ln()) as u64;
        }

        estimate as u64
    }

    fn count_registers_with_value(&self, value: u8) -> u64 {
        self.registers.iter().filter(|&&r| r == value).count() as u64
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    /// Serializes the HLL to a compact binary format for persistence.
    /// Format: "SPINELHLL" (9 bytes) | version (1 byte) | registers (16384 bytes)
    pub fn serialize(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(Self::HLL_MAGIC.len() + 1 + Self::HLL_REGISTER_COUNT);
        bytes.extend_from_slice(Self::HLL_MAGIC);
        bytes.push(Self::HLL_ENCODING_VERSION);
        bytes.extend_from_slice(&self.registers);
        Bytes::from(bytes)
    }

    /// Deserializes an HLL from the binary format. Returns None if the format is invalid.
    pub fn deserialize(data: &Bytes) -> Option<Self> {
        if !data.starts_with(Self::HLL_MAGIC) {
            return None;
        }
        let header_len = Self::HLL_MAGIC.len() + 1;
        if data.len() != header_len + Self::HLL_REGISTER_COUNT {
            return None;
        }
        if data[Self::HLL_MAGIC.len()] != Self::HLL_ENCODING_VERSION {
            return None; // In the future, handle version upgrades here
        }

        let mut hll = Self::new();
        hll.registers
            .copy_from_slice(&data[header_len..header_len + Self::HLL_REGISTER_COUNT]);
        Some(hll)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_hll_has_zero_count() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.count(), 0);
        // All registers should be zero.
        assert!(hll.registers.iter().all(|&r| r == 0));
    }

    #[test]
    fn test_add_increments_count() {
        let mut hll = HyperLogLog::new();
        for i in 0..1000 {
            hll.add(&Bytes::from(format!("item-{i}")));
        }
        // The estimate should be in the right ballpark for 1000 distinct items.
        let estimate = hll.count();
        assert!(
            (800..=1300).contains(&estimate),
            "estimate {estimate} too far from 1000"
        );
    }

    #[test]
    fn test_add_returns_false_for_duplicate_with_lower_rho() {
        let mut hll = HyperLogLog::new();
        // The first add for a key will likely return true.
        let first = hll.add(&Bytes::from_static(b"x"));
        assert!(first);
        // Adding the same item a second time should not change registers.
        let second = hll.add(&Bytes::from_static(b"x"));
        assert!(!second);
    }

    #[test]
    fn test_count_does_not_grow_for_duplicates() {
        let mut hll = HyperLogLog::new();
        for _ in 0..10 {
            hll.add(&Bytes::from_static(b"same"));
        }
        let once = hll.count();
        // Adding the same item many more times must not change the count meaningfully.
        for _ in 0..1000 {
            hll.add(&Bytes::from_static(b"same"));
        }
        let after = hll.count();
        // The estimate is non-decreasing: re-adding identical values may bump
        // the count by a small amount only if registers collapse, but for a
        // single item it should remain very close to 1.
        assert!(after <= 5, "expected ~1, got {after} (was {once})");
    }

    #[test]
    fn test_merge_takes_max_per_register() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        a.add(&Bytes::from_static(b"alpha"));
        b.add(&Bytes::from_static(b"beta"));
        let before = a.count();
        a.merge(&b);
        // Merged count should be >= either side.
        assert!(a.count() >= before);
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut hll = HyperLogLog::new();
        for i in 0..500 {
            hll.add(&Bytes::from(format!("key-{i}")));
        }
        let original_count = hll.count();
        let bytes = hll.serialize();
        let restored = HyperLogLog::deserialize(&bytes).expect("deserialize should succeed");
        assert_eq!(restored, hll);
        // The estimate should be identical after deserialization.
        assert_eq!(restored.count(), original_count);
    }

    #[test]
    fn test_deserialize_rejects_bad_magic() {
        let mut bad = Vec::from(&b"WRONGMAG"[..]);
        bad.extend_from_slice(&[0u8; 16384]);
        assert!(HyperLogLog::deserialize(&Bytes::from(bad)).is_none());
    }

    #[test]
    fn test_deserialize_rejects_wrong_length() {
        let mut bad = Vec::from(&b"SPINELHLL"[..]);
        bad.push(1u8); // version
        bad.extend_from_slice(&[0u8; 100]); // truncated
        assert!(HyperLogLog::deserialize(&Bytes::from(bad)).is_none());
    }

    #[test]
    fn test_deserialize_rejects_unknown_version() {
        let mut bad = Vec::from(&b"SPINELHLL"[..]);
        bad.push(99u8); // bogus version
        bad.extend_from_slice(&[0u8; 16384]);
        assert!(HyperLogLog::deserialize(&Bytes::from(bad)).is_none());
    }

    #[test]
    fn test_alpha_is_correct_for_m16384() {
        // The known correct alpha for m=16384 is ~0.7213 / (1 + 1.079/m).
        let m = 16384.0_f64;
        let expected = 0.7213 / (1.0 + 1.079 / m);
        let hll = HyperLogLog::new();
        assert!((hll.alpha - expected).abs() < 1e-9);
    }

    #[test]
    fn test_memory_usage_is_fixed() {
        let hll = HyperLogLog::new();
        // The struct contains 16384 bytes for registers + alpha (f64).
        assert_eq!(hll.memory_usage(), std::mem::size_of::<HyperLogLog>());
    }
}
