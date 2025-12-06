// tests/property_test.rs

//! Property-based tests for SpinelDB
//!
//! These tests use property-based testing to verify invariants and properties
//! that should always hold, regardless of input values.

// Import TestContext from integration tests
#[path = "integration/test_helpers.rs"]
mod test_helpers;

mod property {
    pub mod consistency_test;
    pub mod roundtrip_test;
    pub mod serialization_test;
}
