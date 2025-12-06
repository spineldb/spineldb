// tests/property/mod.rs

//! Property-based tests for SpinelDB
//!
//! These tests use property-based testing to verify invariants and properties
//! that should always hold, regardless of input values.

pub mod roundtrip_test;
pub mod consistency_test;
pub mod serialization_test;

