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
