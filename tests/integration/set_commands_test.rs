// tests/integration/set_commands_test.rs

//! Integration tests for set commands
//! Tests: SADD, SMEMBERS, SCARD, SISMEMBER, SMISMEMBER, SREM, SPOP, SRANDMEMBER, SMOVE,
//!        SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE

use super::test_helpers::TestContext;
use spineldb::core::{RespValue, SpinelDBError};

// ===== Helper Functions =====

/// Helper to assert that a RespValue is an array with expected string values (unordered)
/// Sets are unordered, so we need to check membership rather than exact order
fn assert_set_equals(result: &RespValue, expected: &[&'static str], message: &str) {
    match result {
        RespValue::Array(values) => {
            assert_eq!(
                values.len(),
                expected.len(),
                "{}: length mismatch, expected {}, got {}",
                message,
                expected.len(),
                values.len()
            );
            let result_set: std::collections::HashSet<String> = values
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(bs) = v {
                        Some(String::from_utf8_lossy(bs).to_string())
                    } else {
                        None
                    }
                })
                .collect();
            let expected_set: std::collections::HashSet<String> =
                expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(
                result_set, expected_set,
                "{}: set mismatch, expected {:?}, got {:?}",
                message, expected_set, result_set
            );
        }
        _ => panic!("{}: Expected array response, got {:?}", message, result),
    }
}

// ===== SADD Tests =====

#[tokio::test]
async fn test_sadd_basic() {
    let ctx = TestContext::new().await;

    // SADD a single member
    let result = ctx.sadd("myset", &["member1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify with SMEMBERS
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(&result, &["member1"], "test_sadd_basic");
}

#[tokio::test]
async fn test_sadd_multiple_members() {
    let ctx = TestContext::new().await;

    // SADD multiple members
    let result = ctx
        .sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify with SMEMBERS
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_sadd_multiple_members",
    );
}

#[tokio::test]
async fn test_sadd_duplicate_members() {
    let ctx = TestContext::new().await;

    // SADD duplicate members (should only count new ones)
    let result = ctx
        .sadd("myset", &["member1", "member2", "member1"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Only 2 new members

    // Verify with SMEMBERS
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2"],
        "test_sadd_duplicate_members",
    );
}

#[tokio::test]
async fn test_sadd_existing_members() {
    let ctx = TestContext::new().await;

    // Add initial members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Add existing members (should return 0)
    let result = ctx.sadd("myset", &["member1", "member2"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify set unchanged
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2"],
        "test_sadd_existing_members",
    );
}

#[tokio::test]
async fn test_sadd_partial_duplicates() {
    let ctx = TestContext::new().await;

    // Add initial members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Add mix of existing and new members
    let result = ctx.sadd("myset", &["member1", "member3"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1)); // Only member3 is new

    // Verify set
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_sadd_partial_duplicates",
    );
}

// Note: SADD requires at least one member, so empty members array is invalid
// This is tested by the parser validation, not execution

#[tokio::test]
async fn test_sadd_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SADD to a string key (should fail)
    let result = ctx.sadd("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SMEMBERS Tests =====

#[tokio::test]
async fn test_smembers_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Get all members
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_smembers_basic",
    );
}

#[tokio::test]
async fn test_smembers_empty_set() {
    let ctx = TestContext::new().await;

    // Get members from non-existent set
    let result = ctx.smembers("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_smembers_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SMEMBERS on a string key (should fail)
    let result = ctx.smembers("mykey").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SCARD Tests =====

#[tokio::test]
async fn test_scard_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Get cardinality
    let result = ctx.scard("myset").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_scard_empty_set() {
    let ctx = TestContext::new().await;

    // Get cardinality of non-existent set
    let result = ctx.scard("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_scard_after_add() {
    let ctx = TestContext::new().await;

    // Initial add
    ctx.sadd("myset", &["member1"]).await.unwrap();
    let result = ctx.scard("myset").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Add more
    ctx.sadd("myset", &["member2", "member3"]).await.unwrap();
    let result = ctx.scard("myset").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_scard_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SCARD on a string key (should fail)
    let result = ctx.scard("mykey").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SISMEMBER Tests =====

#[tokio::test]
async fn test_sismember_exists() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Check existing member
    let result = ctx.sismember("myset", "member1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_sismember_not_exists() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Check non-existing member
    let result = ctx.sismember("myset", "member3").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_sismember_nonexistent_set() {
    let ctx = TestContext::new().await;

    // Check member in non-existent set
    let result = ctx.sismember("nonexistent", "member1").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_sismember_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SISMEMBER on a string key (should fail)
    let result = ctx.sismember("mykey", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SMISMEMBER Tests =====

#[tokio::test]
async fn test_smismember_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Check multiple members
    let result = ctx
        .smismember("myset", &["member1", "member2", "member4"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::Integer(1)); // member1 exists
            assert_eq!(values[1], RespValue::Integer(1)); // member2 exists
            assert_eq!(values[2], RespValue::Integer(0)); // member4 doesn't exist
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_smismember_all_exist() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Check all existing members
    let result = ctx
        .smismember("myset", &["member1", "member2"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::Integer(1));
            assert_eq!(values[1], RespValue::Integer(1));
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_smismember_none_exist() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Check non-existing members
    let result = ctx
        .smismember("myset", &["member3", "member4"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::Integer(0));
            assert_eq!(values[1], RespValue::Integer(0));
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_smismember_nonexistent_set() {
    let ctx = TestContext::new().await;

    // Check members in non-existent set
    let result = ctx
        .smismember("nonexistent", &["member1", "member2"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::Integer(0));
            assert_eq!(values[1], RespValue::Integer(0));
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_smismember_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SMISMEMBER on a string key (should fail)
    let result = ctx.smismember("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SREM Tests =====

#[tokio::test]
async fn test_srem_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Remove one member
    let result = ctx.srem("myset", &["member1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify removed
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(&result, &["member2", "member3"], "test_srem_basic");
}

#[tokio::test]
async fn test_srem_multiple_members() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3", "member4"])
        .await
        .unwrap();

    // Remove multiple members
    let result = ctx.srem("myset", &["member1", "member3"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(2));

    // Verify removed
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member2", "member4"],
        "test_srem_multiple_members",
    );
}

#[tokio::test]
async fn test_srem_nonexistent_members() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Remove non-existing members
    let result = ctx.srem("myset", &["member3", "member4"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify set unchanged
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2"],
        "test_srem_nonexistent_members",
    );
}

#[tokio::test]
async fn test_srem_partial_existing() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Remove mix of existing and non-existing
    let result = ctx.srem("myset", &["member1", "member4"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1)); // Only member1 was removed

    // Verify
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["member2", "member3"],
        "test_srem_partial_existing",
    );
}

#[tokio::test]
async fn test_srem_all_members() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Remove all members
    let result = ctx.srem("myset", &["member1", "member2"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(2));

    // Verify set is empty (should be deleted)
    let result = ctx.smembers("myset").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
    let result = ctx.scard("myset").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_srem_nonexistent_set() {
    let ctx = TestContext::new().await;

    // Remove from non-existent set
    let result = ctx.srem("nonexistent", &["member1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_srem_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SREM on a string key (should fail)
    let result = ctx.srem("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SPOP Tests =====

#[tokio::test]
async fn test_spop_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Pop one member
    let result = ctx.spop("myset", None).await.unwrap();
    match result {
        RespValue::BulkString(_) => {
            // Verify count decreased
            let count = ctx.scard("myset").await.unwrap();
            assert_eq!(count, RespValue::Integer(2));
        }
        _ => panic!("Expected BulkString response"),
    }
}

#[tokio::test]
async fn test_spop_with_count() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3", "member4"])
        .await
        .unwrap();

    // Pop 2 members
    let result = ctx.spop("myset", Some(2)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            // Verify count decreased
            let count = ctx.scard("myset").await.unwrap();
            assert_eq!(count, RespValue::Integer(2));
        }
        _ => panic!("Expected Array response"),
    }
}

#[tokio::test]
async fn test_spop_all_members() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Pop all members
    let result = ctx.spop("myset", Some(2)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            // Verify set is empty
            let count = ctx.scard("myset").await.unwrap();
            assert_eq!(count, RespValue::Integer(0));
        }
        _ => panic!("Expected Array response"),
    }
}

#[tokio::test]
async fn test_spop_more_than_exists() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Pop more than exists
    let result = ctx.spop("myset", Some(5)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2); // Only 2 members available
            // Verify set is empty
            let count = ctx.scard("myset").await.unwrap();
            assert_eq!(count, RespValue::Integer(0));
        }
        _ => panic!("Expected Array response"),
    }
}

#[tokio::test]
async fn test_spop_empty_set() {
    let ctx = TestContext::new().await;

    // Pop from non-existent set
    let result = ctx.spop("nonexistent", None).await.unwrap();
    assert_eq!(result, RespValue::Null);

    // Pop with count from non-existent set
    let result = ctx.spop("nonexistent", Some(2)).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_spop_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SPOP on a string key (should fail)
    let result = ctx.spop("mykey", None).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SRANDMEMBER Tests =====

#[tokio::test]
async fn test_srandmember_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Get random member
    let result = ctx.srandmember("myset", None).await.unwrap();
    match result {
        RespValue::BulkString(_) => {
            // Verify set unchanged
            let count = ctx.scard("myset").await.unwrap();
            assert_eq!(count, RespValue::Integer(3));
        }
        _ => panic!("Expected BulkString response"),
    }
}

#[tokio::test]
async fn test_srandmember_with_positive_count() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // Get 2 random members
    let result = ctx.srandmember("myset", Some(2)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            // Verify set unchanged
            let count = ctx.scard("myset").await.unwrap();
            assert_eq!(count, RespValue::Integer(3));
        }
        _ => panic!("Expected Array response"),
    }
}

#[tokio::test]
async fn test_srandmember_with_negative_count() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Get -3 random members (allows duplicates)
    let result = ctx.srandmember("myset", Some(-3)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 3); // Can have duplicates
            // Verify set unchanged
            let count = ctx.scard("myset").await.unwrap();
            assert_eq!(count, RespValue::Integer(2));
        }
        _ => panic!("Expected Array response"),
    }
}

#[tokio::test]
async fn test_srandmember_empty_set() {
    let ctx = TestContext::new().await;

    // Get random member from non-existent set
    let result = ctx.srandmember("nonexistent", None).await.unwrap();
    assert_eq!(result, RespValue::Null);

    // Get random members with count from non-existent set
    let result = ctx.srandmember("nonexistent", Some(2)).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_srandmember_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SRANDMEMBER on a string key (should fail)
    let result = ctx.srandmember("mykey", None).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SMOVE Tests =====

#[tokio::test]
async fn test_smove_basic() {
    let ctx = TestContext::new().await;

    // Add members to source
    ctx.sadd("source", &["member1", "member2"]).await.unwrap();

    // Move member to destination
    let result = ctx.smove("source", "destination", "member1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify source
    let result = ctx.smembers("source").await.unwrap();
    assert_set_equals(&result, &["member2"], "test_smove_basic source");

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(&result, &["member1"], "test_smove_basic destination");
}

#[tokio::test]
async fn test_smove_nonexistent_member() {
    let ctx = TestContext::new().await;

    // Add members to source
    ctx.sadd("source", &["member1", "member2"]).await.unwrap();

    // Move non-existing member
    let result = ctx.smove("source", "destination", "member3").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify source unchanged
    let result = ctx.smembers("source").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2"],
        "test_smove_nonexistent_member source",
    );

    // Verify destination empty
    let result = ctx.smembers("destination").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_smove_to_existing_destination() {
    let ctx = TestContext::new().await;

    // Add members to both sets
    ctx.sadd("source", &["member1", "member2"]).await.unwrap();
    ctx.sadd("destination", &["member3"]).await.unwrap();

    // Move member
    let result = ctx.smove("source", "destination", "member1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify source
    let result = ctx.smembers("source").await.unwrap();
    assert_set_equals(
        &result,
        &["member2"],
        "test_smove_to_existing_destination source",
    );

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member3"],
        "test_smove_to_existing_destination destination",
    );
}

#[tokio::test]
async fn test_smove_same_source_destination() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // Move to same set
    // SMOVE behavior: adds to destination first, then removes from source
    // When source == destination: member is added (but already exists, so insert returns false),
    // then removed from source. So the member effectively gets removed.
    let result = ctx.smove("myset", "myset", "member1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify member1 was removed (SMOVE removes from source even if source == destination)
    let result = ctx.smembers("myset").await.unwrap();
    match result {
        RespValue::Array(values) => {
            // Set should have 1 member (member1 was removed)
            assert_eq!(
                values.len(),
                1,
                "Set should have 1 member after SMOVE to itself"
            );
            // Verify member2 is still present
            let members: std::collections::HashSet<String> = values
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(bs) = v {
                        Some(String::from_utf8_lossy(bs).to_string())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(
                members.contains("member2"),
                "member2 should still be present"
            );
            assert!(!members.contains("member1"), "member1 should be removed");
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_smove_nonexistent_source() {
    let ctx = TestContext::new().await;

    // Move from non-existent source
    let result = ctx
        .smove("nonexistent", "destination", "member1")
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination empty
    let result = ctx.smembers("destination").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_smove_type_error_source() {
    let ctx = TestContext::new().await;

    // Create a string key as source
    ctx.set("source", "value").await.unwrap();

    // Try to SMOVE from a string key (should fail)
    let result = ctx.smove("source", "destination", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_smove_type_error_destination() {
    let ctx = TestContext::new().await;

    // Add to source
    ctx.sadd("source", &["member1"]).await.unwrap();

    // Create a string key as destination
    ctx.set("destination", "value").await.unwrap();

    // Try to SMOVE to a string key (should fail)
    let result = ctx.smove("source", "destination", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SINTER Tests =====

#[tokio::test]
async fn test_sinter_basic() {
    let ctx = TestContext::new().await;

    // Create sets with common members
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2", "member3", "member4"])
        .await
        .unwrap();

    // Get intersection
    let result = ctx.sinter(&["set1", "set2"]).await.unwrap();
    assert_set_equals(&result, &["member2", "member3"], "test_sinter_basic");
}

#[tokio::test]
async fn test_sinter_no_intersection() {
    let ctx = TestContext::new().await;

    // Create sets with no common members
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member3", "member4"]).await.unwrap();

    // Get intersection
    let result = ctx.sinter(&["set1", "set2"]).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_sinter_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2", "member3", "member4"])
        .await
        .unwrap();
    ctx.sadd("set3", &["member2", "member5"]).await.unwrap();

    // Get intersection of all three
    let result = ctx.sinter(&["set1", "set2", "set3"]).await.unwrap();
    assert_set_equals(&result, &["member2"], "test_sinter_three_sets");
}

#[tokio::test]
async fn test_sinter_with_empty_set() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    // set2 doesn't exist (empty)

    // Get intersection
    let result = ctx.sinter(&["set1", "set2"]).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

// Note: SINTER with single set requires multi-key lock, so we skip this test
// Single set operations are handled by SMEMBERS instead

#[tokio::test]
async fn test_sinter_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SINTER with a string key (should fail)
    let result = ctx.sinter(&["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SUNION Tests =====

#[tokio::test]
async fn test_sunion_basic() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();

    // Get union
    let result = ctx.sunion(&["set1", "set2"]).await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_sunion_basic",
    );
}

#[tokio::test]
async fn test_sunion_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();
    ctx.sadd("set3", &["member3", "member4"]).await.unwrap();

    // Get union of all three
    let result = ctx.sunion(&["set1", "set2", "set3"]).await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3", "member4"],
        "test_sunion_three_sets",
    );
}

#[tokio::test]
async fn test_sunion_with_empty_set() {
    let ctx = TestContext::new().await;

    // Create set
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    // set2 doesn't exist (empty)

    // Get union
    let result = ctx.sunion(&["set1", "set2"]).await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2"],
        "test_sunion_with_empty_set",
    );
}

#[tokio::test]
async fn test_sunion_all_empty() {
    let ctx = TestContext::new().await;

    // Get union of non-existent sets
    let result = ctx.sunion(&["set1", "set2"]).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

// Note: SUNION with single set requires multi-key lock, so we skip this test
// Single set operations are handled by SMEMBERS instead

#[tokio::test]
async fn test_sunion_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SUNION with a string key (should fail)
    let result = ctx.sunion(&["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SDIFF Tests =====

#[tokio::test]
async fn test_sdiff_basic() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();

    // Get difference (set1 - set2)
    let result = ctx.sdiff(&["set1", "set2"]).await.unwrap();
    assert_set_equals(&result, &["member1"], "test_sdiff_basic");
}

#[tokio::test]
async fn test_sdiff_no_difference() {
    let ctx = TestContext::new().await;

    // Create sets with same members
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member1", "member2"]).await.unwrap();

    // Get difference
    let result = ctx.sdiff(&["set1", "set2"]).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_sdiff_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2"]).await.unwrap();
    ctx.sadd("set3", &["member3"]).await.unwrap();

    // Get difference (set1 - set2 - set3)
    let result = ctx.sdiff(&["set1", "set2", "set3"]).await.unwrap();
    assert_set_equals(&result, &["member1"], "test_sdiff_three_sets");
}

#[tokio::test]
async fn test_sdiff_with_empty_set() {
    let ctx = TestContext::new().await;

    // Create set
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    // set2 doesn't exist (empty)

    // Get difference
    let result = ctx.sdiff(&["set1", "set2"]).await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2"],
        "test_sdiff_with_empty_set",
    );
}

#[tokio::test]
async fn test_sdiff_first_empty() {
    let ctx = TestContext::new().await;

    // Create set
    ctx.sadd("set2", &["member1", "member2"]).await.unwrap();
    // set1 doesn't exist (empty)

    // Get difference
    let result = ctx.sdiff(&["set1", "set2"]).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

// Note: SDIFF with single set requires multi-key lock, so we skip this test
// Single set operations are handled by SMEMBERS instead

#[tokio::test]
async fn test_sdiff_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SDIFF with a string key (should fail)
    let result = ctx.sdiff(&["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SINTERSTORE Tests =====

#[tokio::test]
async fn test_sinterstore_basic() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2", "member3", "member4"])
        .await
        .unwrap();

    // Store intersection
    let result = ctx
        .sinterstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2));

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(&result, &["member2", "member3"], "test_sinterstore_basic");
}

#[tokio::test]
async fn test_sinterstore_overwrite_destination() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();
    ctx.sadd("destination", &["old1", "old2"]).await.unwrap();

    // Store intersection (should overwrite destination)
    let result = ctx
        .sinterstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify destination overwritten
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(
        &result,
        &["member2"],
        "test_sinterstore_overwrite_destination",
    );
}

#[tokio::test]
async fn test_sinterstore_no_intersection() {
    let ctx = TestContext::new().await;

    // Create sets with no intersection
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member3", "member4"]).await.unwrap();

    // Store intersection
    let result = ctx
        .sinterstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination is empty
    let result = ctx.smembers("destination").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_sinterstore_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2", "member3", "member4"])
        .await
        .unwrap();
    ctx.sadd("set3", &["member2", "member5"]).await.unwrap();

    // Store intersection
    let result = ctx
        .sinterstore("destination", &["set1", "set2", "set3"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(&result, &["member2"], "test_sinterstore_three_sets");
}

#[tokio::test]
async fn test_sinterstore_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SINTERSTORE with a string key (should fail)
    let result = ctx.sinterstore("destination", &["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SUNIONSTORE Tests =====

#[tokio::test]
async fn test_sunionstore_basic() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();

    // Store union
    let result = ctx
        .sunionstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_sunionstore_basic",
    );
}

#[tokio::test]
async fn test_sunionstore_overwrite_destination() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();
    ctx.sadd("destination", &["old1"]).await.unwrap();

    // Store union (should overwrite destination)
    let result = ctx
        .sunionstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify destination overwritten
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_sunionstore_overwrite_destination",
    );
}

#[tokio::test]
async fn test_sunionstore_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();
    ctx.sadd("set3", &["member3", "member4"]).await.unwrap();

    // Store union
    let result = ctx
        .sunionstore("destination", &["set1", "set2", "set3"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(4));

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2", "member3", "member4"],
        "test_sunionstore_three_sets",
    );
}

#[tokio::test]
async fn test_sunionstore_with_empty_sets() {
    let ctx = TestContext::new().await;

    // Create set
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    // set2 doesn't exist

    // Store union
    let result = ctx
        .sunionstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2));

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member2"],
        "test_sunionstore_with_empty_sets",
    );
}

#[tokio::test]
async fn test_sunionstore_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SUNIONSTORE with a string key (should fail)
    let result = ctx.sunionstore("destination", &["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== SDIFFSTORE Tests =====

#[tokio::test]
async fn test_sdiffstore_basic() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2", "member3"]).await.unwrap();

    // Store difference
    let result = ctx
        .sdiffstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(&result, &["member1"], "test_sdiffstore_basic");
}

#[tokio::test]
async fn test_sdiffstore_overwrite_destination() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2"]).await.unwrap();
    ctx.sadd("destination", &["old1", "old2"]).await.unwrap();

    // Store difference (should overwrite destination)
    let result = ctx
        .sdiffstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2));

    // Verify destination overwritten
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(
        &result,
        &["member1", "member3"],
        "test_sdiffstore_overwrite_destination",
    );
}

#[tokio::test]
async fn test_sdiffstore_no_difference() {
    let ctx = TestContext::new().await;

    // Create sets with same members
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member1", "member2"]).await.unwrap();

    // Store difference
    let result = ctx
        .sdiffstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination is empty
    let result = ctx.smembers("destination").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_sdiffstore_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.sadd("set1", &["member1", "member2", "member3"])
        .await
        .unwrap();
    ctx.sadd("set2", &["member2"]).await.unwrap();
    ctx.sadd("set3", &["member3"]).await.unwrap();

    // Store difference
    let result = ctx
        .sdiffstore("destination", &["set1", "set2", "set3"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify destination
    let result = ctx.smembers("destination").await.unwrap();
    assert_set_equals(&result, &["member1"], "test_sdiffstore_three_sets");
}

#[tokio::test]
async fn test_sdiffstore_type_error() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SDIFFSTORE with a string key (should fail)
    let result = ctx.sdiffstore("destination", &["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== Edge Cases and Error Handling =====

#[tokio::test]
async fn test_set_operations_with_large_sets() {
    let ctx = TestContext::new().await;

    // Create large sets
    let mut members1 = Vec::new();
    let mut members2 = Vec::new();
    for i in 0..100 {
        members1.push(format!("member{}", i));
        if i % 2 == 0 {
            members2.push(format!("member{}", i));
        }
    }

    ctx.sadd(
        "set1",
        &members1.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    ctx.sadd(
        "set2",
        &members2.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )
    .await
    .unwrap();

    // Test intersection
    let result = ctx.sinter(&["set1", "set2"]).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 50); // Half of 100
        }
        _ => panic!("Expected array response"),
    }

    // Test union
    let result = ctx.sunion(&["set1", "set2"]).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 100); // All unique members
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_set_operations_with_unicode() {
    let ctx = TestContext::new().await;

    // Add unicode members
    ctx.sadd("myset", &["成员1", "member2", "🌍"])
        .await
        .unwrap();

    // Verify
    let result = ctx.smembers("myset").await.unwrap();
    assert_set_equals(
        &result,
        &["成员1", "member2", "🌍"],
        "test_set_operations_with_unicode",
    );
}

#[tokio::test]
async fn test_set_operations_with_empty_strings() {
    let ctx = TestContext::new().await;

    // Add empty string as member
    ctx.sadd("myset", &["", "member1"]).await.unwrap();

    // Verify
    let result = ctx.smembers("myset").await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            // Check that empty string is present
            let has_empty = values.iter().any(|v| {
                if let RespValue::BulkString(bs) = v {
                    bs.is_empty()
                } else {
                    false
                }
            });
            assert!(has_empty, "Empty string member should be present");
        }
        _ => panic!("Expected array response"),
    }
}

// ===== Additional Coverage Tests =====

#[tokio::test]
async fn test_sadd_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SADD to a string key (should fail)
    let result = ctx.sadd("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_scard_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SCARD on a string key (should fail)
    let result = ctx.scard("mykey").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_smembers_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SMEMBERS on a string key (should fail)
    let result = ctx.smembers("mykey").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_sismember_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SISMEMBER on a string key (should fail)
    let result = ctx.sismember("mykey", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_smismember_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SMISMEMBER on a string key (should fail)
    let result = ctx.smismember("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_srem_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SREM on a string key (should fail)
    let result = ctx.srem("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_spop_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SPOP on a string key (should fail)
    let result = ctx.spop("mykey", Some(1)).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_srandmember_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to SRANDMEMBER on a string key (should fail)
    let result = ctx.srandmember("mykey", Some(1)).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_smove_wrong_type_source() {
    let ctx = TestContext::new().await;

    // Create a string key as source
    ctx.set("source", "value").await.unwrap();
    ctx.sadd("dest", &["member1"]).await.unwrap();

    // Try to SMOVE from a string key (should fail)
    let result = ctx.smove("source", "dest", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_smove_wrong_type_destination() {
    let ctx = TestContext::new().await;

    // Create a string key as destination
    ctx.sadd("source", &["member1"]).await.unwrap();
    ctx.set("dest", "value").await.unwrap();

    // Try to SMOVE to a string key (should fail)
    let result = ctx.smove("source", "dest", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_sinter_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SINTER with a string key (should fail)
    let result = ctx.sinter(&["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_sunion_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SUNION with a string key (should fail)
    let result = ctx.sunion(&["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_sdiff_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SDIFF with a string key (should fail)
    let result = ctx.sdiff(&["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_sinterstore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SINTERSTORE with a string key (should fail)
    let result = ctx.sinterstore("destination", &["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_sunionstore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.sadd("set1", &["member1"]).await.unwrap();

    // Try to SUNIONSTORE with a string key (should fail)
    let result = ctx.sunionstore("destination", &["mykey", "set1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// Note: test_spop_empty_set already exists earlier in the file

#[tokio::test]
async fn test_spop_count_zero() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // SPOP with count 0 should return empty array
    let result = ctx.spop("myset", Some(0)).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_spop_count_larger_than_set() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // SPOP with count larger than set size should return all members
    let result = ctx.spop("myset", Some(10)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
        }
        _ => panic!("Expected array response"),
    }
}

// Note: test_srandmember_empty_set already exists earlier in the file

#[tokio::test]
async fn test_srandmember_count_zero() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // SRANDMEMBER with count 0 should return empty array
    let result = ctx.srandmember("myset", Some(0)).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_srandmember_count_larger_than_set() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2"]).await.unwrap();

    // SRANDMEMBER with count larger than set size should return all members
    let result = ctx.srandmember("myset", Some(10)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_srandmember_negative_count() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.sadd("myset", &["member1", "member2", "member3"])
        .await
        .unwrap();

    // SRANDMEMBER with negative count should return members (may have duplicates)
    let result = ctx.srandmember("myset", Some(-5)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 5); // Should return exactly 5 members (with possible duplicates)
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_sinterstore_empty_result() {
    let ctx = TestContext::new().await;

    // Create sets with no intersection
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member3", "member4"]).await.unwrap();

    // Store intersection (should be empty)
    let result = ctx
        .sinterstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination doesn't exist (empty result deletes destination)
    let result = ctx.scard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_sunionstore_empty_sets() {
    let ctx = TestContext::new().await;

    // Create empty sets
    ctx.sadd("set1", &["member1"]).await.unwrap();
    ctx.srem("set1", &["member1"]).await.unwrap();

    // Store union of empty sets
    let result = ctx.sunionstore("destination", &["set1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination doesn't exist
    let result = ctx.scard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_sdiffstore_empty_result() {
    let ctx = TestContext::new().await;

    // Create sets where difference is empty
    ctx.sadd("set1", &["member1", "member2"]).await.unwrap();
    ctx.sadd("set2", &["member1", "member2"]).await.unwrap();

    // Store difference (should be empty)
    let result = ctx
        .sdiffstore("destination", &["set1", "set2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination doesn't exist
    let result = ctx.scard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}
