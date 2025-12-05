// tests/integration_test.rs

//! Integration tests for SpinelDB
//!
//! These tests execute commands end-to-end with a real database instance,
//! verifying command execution, state changes, and data consistency.

mod integration {
    pub mod cache_test;
    pub mod fixtures;
    pub mod geospatial_test;
    pub mod hash_commands_test;
    pub mod json_commands_test;
    pub mod list_commands_test;
    pub mod persistence_test;
    pub mod pubsub_test;
    pub mod replication_test;
    pub mod set_commands_test;
    pub mod string_commands_test;
    pub mod test_helpers;
    pub mod transaction_test;
    pub mod zset_commands_test;
}
