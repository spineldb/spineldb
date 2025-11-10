// tests/integration/hash_commands_test.rs

//! Integration tests for hash commands
//! Tests: HSET, HGET, HDEL, HGETALL, HMGET, HEXISTS, HSETNX, HLEN, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSTRLEN, HRANDFIELD

use super::test_helpers::TestContext;
use bytes::Bytes;
use spineldb::core::{RespValue, SpinelDBError};

// ===== Helper Functions =====

/// Helper to assert that a RespValue is an array with expected field-value pairs
fn assert_hgetall_equals(
    result: &RespValue,
    expected: &[(&'static str, &'static str)],
    message: &str,
) {
    match result {
        RespValue::Array(values) => {
            assert_eq!(
                values.len(),
                expected.len() * 2,
                "{}: length mismatch, expected {} pairs ({} elements), got {}",
                message,
                expected.len(),
                expected.len() * 2,
                values.len()
            );
            for (i, (field, value)) in expected.iter().enumerate() {
                let field_idx = i * 2;
                let value_idx = i * 2 + 1;
                let expected_field = RespValue::BulkString(Bytes::from(*field));
                let expected_value = RespValue::BulkString(Bytes::from(*value));
                assert_eq!(
                    &values[field_idx], &expected_field,
                    "{}: field mismatch at index {}, expected '{}', got {:?}",
                    message, field_idx, field, values[field_idx]
                );
                assert_eq!(
                    &values[value_idx], &expected_value,
                    "{}: value mismatch at index {}, expected '{}', got {:?}",
                    message, value_idx, value, values[value_idx]
                );
            }
        }
        _ => panic!("{}: Expected array response, got {:?}", message, result),
    }
}

/// Helper to assert that a RespValue is an array with expected string values
fn assert_array_equals(result: &RespValue, expected: &[&'static str], message: &str) {
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
            for (i, (actual, expected_str)) in values.iter().zip(expected.iter()).enumerate() {
                let expected_value = RespValue::BulkString(Bytes::from(*expected_str));
                assert_eq!(
                    actual, &expected_value,
                    "{}: mismatch at index {}, expected '{}', got {:?}",
                    message, i, expected_str, actual
                );
            }
        }
        _ => panic!("{}: Expected array response, got {:?}", message, result),
    }
}

// ===== HSET Tests =====

#[tokio::test]
async fn test_hset_basic() {
    let ctx = TestContext::new().await;

    // HSET a single field-value pair
    let result = ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify with HGET
    let result = ctx.hget("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value1")));
}

#[tokio::test]
async fn test_hset_multiple_fields() {
    let ctx = TestContext::new().await;

    // HSET multiple field-value pairs
    let result = ctx
        .hset(
            "myhash",
            &[
                ("field1", "value1"),
                ("field2", "value2"),
                ("field3", "value3"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify all fields
    let result = ctx.hget("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value1")));
    let result = ctx.hget("myhash", "field2").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value2")));
    let result = ctx.hget("myhash", "field3").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value3")));
}

#[tokio::test]
async fn test_hset_update_existing_field() {
    let ctx = TestContext::new().await;

    // HSET initial field
    let result = ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Update the same field
    let result = ctx.hset("myhash", &[("field1", "value2")]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0)); // 0 new fields, 1 updated

    // Verify the update
    let result = ctx.hget("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value2")));
}

#[tokio::test]
async fn test_hset_mixed_new_and_existing() {
    let ctx = TestContext::new().await;

    // HSET initial fields
    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // HSET with one existing and one new field
    let result = ctx
        .hset("myhash", &[("field1", "updated1"), ("field3", "value3")])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1)); // 1 new field

    // Verify all fields
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("updated1"))
    );
    assert_eq!(
        ctx.hget("myhash", "field2").await.unwrap(),
        RespValue::BulkString(Bytes::from("value2"))
    );
    assert_eq!(
        ctx.hget("myhash", "field3").await.unwrap(),
        RespValue::BulkString(Bytes::from("value3"))
    );
}

// ===== HGET Tests =====

#[tokio::test]
async fn test_hget_existing_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hget("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value1")));
}

#[tokio::test]
async fn test_hget_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.hget("nonexistent", "field1").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_hget_nonexistent_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hget("myhash", "nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_hget_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mystring", "value").await.unwrap();

    // Try to HGET from a string
    let result = ctx.hget("mystring", "field1").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HDEL Tests =====

#[tokio::test]
async fn test_hdel_single_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    let result = ctx.hdel("myhash", &["field1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify field1 is deleted
    assert_eq!(ctx.hget("myhash", "field1").await.unwrap(), RespValue::Null);
    // Verify field2 still exists
    assert_eq!(
        ctx.hget("myhash", "field2").await.unwrap(),
        RespValue::BulkString(Bytes::from("value2"))
    );
}

#[tokio::test]
async fn test_hdel_multiple_fields() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hdel("myhash", &["field1", "field3"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(2));

    // Verify deleted fields
    assert_eq!(ctx.hget("myhash", "field1").await.unwrap(), RespValue::Null);
    assert_eq!(ctx.hget("myhash", "field3").await.unwrap(), RespValue::Null);
    // Verify remaining field
    assert_eq!(
        ctx.hget("myhash", "field2").await.unwrap(),
        RespValue::BulkString(Bytes::from("value2"))
    );
}

#[tokio::test]
async fn test_hdel_nonexistent_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hdel("myhash", &["nonexistent"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hdel_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.hdel("nonexistent", &["field1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hdel_removes_empty_hash() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hdel("myhash", &["field1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Hash should be removed
    let result = ctx.hlen("myhash").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hdel_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hdel("mystring", &["field1"]).await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HGETALL Tests =====

#[tokio::test]
async fn test_hgetall_basic() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    let result = ctx.hgetall("myhash").await.unwrap();
    assert_hgetall_equals(
        &result,
        &[("field1", "value1"), ("field2", "value2")],
        "test_hgetall_basic",
    );
}

#[tokio::test]
async fn test_hgetall_empty_hash() {
    let ctx = TestContext::new().await;

    let result = ctx.hgetall("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_hgetall_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hgetall("mystring").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

#[tokio::test]
async fn test_hgetall_preserves_order() {
    let ctx = TestContext::new().await;

    // Insert fields in specific order
    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
            ("field4", "value4"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hgetall("myhash").await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 8);
            // Check that fields are in order
            assert_eq!(values[0], RespValue::BulkString(Bytes::from("field1")));
            assert_eq!(values[2], RespValue::BulkString(Bytes::from("field2")));
            assert_eq!(values[4], RespValue::BulkString(Bytes::from("field3")));
            assert_eq!(values[6], RespValue::BulkString(Bytes::from("field4")));
        }
        _ => panic!("Expected array"),
    }
}

// ===== HMGET Tests =====

#[tokio::test]
async fn test_hmget_basic() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hmget("myhash", &["field1", "field2"]).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::BulkString(Bytes::from("value1")));
            assert_eq!(values[1], RespValue::BulkString(Bytes::from("value2")));
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hmget_with_nonexistent_fields() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx
        .hmget("myhash", &["field1", "nonexistent", "field2"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Bytes::from("value1")));
            assert_eq!(values[1], RespValue::Null);
            assert_eq!(values[2], RespValue::Null);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hmget_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx
        .hmget("nonexistent", &["field1", "field2"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::Null);
            assert_eq!(values[1], RespValue::Null);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hmget_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hmget("mystring", &["field1"]).await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HEXISTS Tests =====

#[tokio::test]
async fn test_hexists_existing_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hexists("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_hexists_nonexistent_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hexists("myhash", "nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hexists_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.hexists("nonexistent", "field1").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hexists_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hexists("mystring", "field1").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HSETNX Tests =====

#[tokio::test]
async fn test_hsetnx_new_field() {
    let ctx = TestContext::new().await;

    let result = ctx.hsetnx("myhash", "field1", "value1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify the field was set
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("value1"))
    );
}

#[tokio::test]
async fn test_hsetnx_existing_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hsetnx("myhash", "field1", "value2").await.unwrap();
    assert_eq!(result, RespValue::Integer(0)); // Field already exists, not set

    // Verify the original value is unchanged
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("value1"))
    );
}

#[tokio::test]
async fn test_hsetnx_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hsetnx("mystring", "field1", "value1").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HLEN Tests =====

#[tokio::test]
async fn test_hlen_basic() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hlen("myhash").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_hlen_empty_hash() {
    let ctx = TestContext::new().await;

    let result = ctx.hlen("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hlen_after_delete() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    ctx.hdel("myhash", &["field1"]).await.unwrap();

    let result = ctx.hlen("myhash").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_hlen_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hlen("mystring").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HKEYS Tests =====

#[tokio::test]
async fn test_hkeys_basic() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hkeys("myhash").await.unwrap();
    assert_array_equals(&result, &["field1", "field2", "field3"], "test_hkeys_basic");
}

#[tokio::test]
async fn test_hkeys_empty_hash() {
    let ctx = TestContext::new().await;

    let result = ctx.hkeys("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_hkeys_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hkeys("mystring").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HVALS Tests =====

#[tokio::test]
async fn test_hvals_basic() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hvals("myhash").await.unwrap();
    assert_array_equals(&result, &["value1", "value2", "value3"], "test_hvals_basic");
}

#[tokio::test]
async fn test_hvals_empty_hash() {
    let ctx = TestContext::new().await;

    let result = ctx.hvals("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_hvals_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hvals("mystring").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HINCRBY Tests =====

#[tokio::test]
async fn test_hincrby_new_field() {
    let ctx = TestContext::new().await;

    let result = ctx.hincrby("myhash", "field1", 5).await.unwrap();
    assert_eq!(result, RespValue::Integer(5));

    // Verify the value
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("5"))
    );
}

#[tokio::test]
async fn test_hincrby_existing_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "10")]).await.unwrap();

    let result = ctx.hincrby("myhash", "field1", 5).await.unwrap();
    assert_eq!(result, RespValue::Integer(15));

    // Verify the value
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("15"))
    );
}

#[tokio::test]
async fn test_hincrby_negative_increment() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "10")]).await.unwrap();

    let result = ctx.hincrby("myhash", "field1", -5).await.unwrap();
    assert_eq!(result, RespValue::Integer(5));
}

#[tokio::test]
async fn test_hincrby_zero_increment() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "10")]).await.unwrap();

    let result = ctx.hincrby("myhash", "field1", 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(10));
}

#[tokio::test]
async fn test_hincrby_non_integer_value() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "not_a_number")])
        .await
        .unwrap();

    let result = ctx.hincrby("myhash", "field1", 5).await;
    assert!(matches!(result, Err(SpinelDBError::NotAnInteger)));
}

#[tokio::test]
async fn test_hincrby_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hincrby("mystring", "field1", 5).await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

#[tokio::test]
async fn test_hincrby_overflow() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "9223372036854775807")])
        .await
        .unwrap(); // i64::MAX

    let result = ctx.hincrby("myhash", "field1", 1).await;
    assert!(matches!(result, Err(SpinelDBError::Overflow)));
}

// ===== HINCRBYFLOAT Tests =====

#[tokio::test]
async fn test_hincrbyfloat_new_field() {
    let ctx = TestContext::new().await;

    let result = ctx.hincrbyfloat("myhash", "field1", 5.5).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 5.5).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hincrbyfloat_existing_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "10.5")]).await.unwrap();

    let result = ctx.hincrbyfloat("myhash", "field1", 5.5).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 16.0).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hincrbyfloat_negative_increment() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "10.5")]).await.unwrap();

    let result = ctx.hincrbyfloat("myhash", "field1", -5.5).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 5.0).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hincrbyfloat_non_float_value() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "not_a_number")])
        .await
        .unwrap();

    let result = ctx.hincrbyfloat("myhash", "field1", 5.5).await;
    assert!(matches!(result, Err(SpinelDBError::NotAFloat)));
}

#[tokio::test]
async fn test_hincrbyfloat_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hincrbyfloat("mystring", "field1", 5.5).await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

#[tokio::test]
async fn test_hincrbyfloat_integer_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "10")]).await.unwrap();

    let result = ctx.hincrbyfloat("myhash", "field1", 5.5).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 15.5).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

// ===== HSTRLEN Tests =====

#[tokio::test]
async fn test_hstrlen_existing_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hstrlen("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::Integer(6)); // "value1" has 6 characters
}

#[tokio::test]
async fn test_hstrlen_nonexistent_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    let result = ctx.hstrlen("myhash", "nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hstrlen_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.hstrlen("nonexistent", "field1").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hstrlen_empty_string() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "")]).await.unwrap();

    let result = ctx.hstrlen("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hstrlen_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hstrlen("mystring", "field1").await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== HRANDFIELD Tests =====

#[tokio::test]
async fn test_hrandfield_basic() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hrandfield("myhash", None, false).await.unwrap();
    match result {
        RespValue::BulkString(field) => {
            let field_str = String::from_utf8(field.to_vec()).unwrap();
            assert!(field_str == "field1" || field_str == "field2" || field_str == "field3");
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hrandfield_with_count() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hrandfield("myhash", Some(2), false).await.unwrap();
    match result {
        RespValue::Array(fields) => {
            assert_eq!(fields.len(), 2);
            for field in fields {
                match field {
                    RespValue::BulkString(f) => {
                        let field_str = String::from_utf8(f.to_vec()).unwrap();
                        assert!(
                            field_str == "field1" || field_str == "field2" || field_str == "field3"
                        );
                    }
                    _ => panic!("Expected bulk string in array"),
                }
            }
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_with_values() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    let result = ctx.hrandfield("myhash", None, true).await.unwrap();
    match result {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            match (&items[0], &items[1]) {
                (RespValue::BulkString(field), RespValue::BulkString(value)) => {
                    let field_str = String::from_utf8(field.to_vec()).unwrap();
                    let value_str = String::from_utf8(value.to_vec()).unwrap();
                    assert!(field_str == "field1" || field_str == "field2");
                    assert!(value_str == "value1" || value_str == "value2");
                }
                _ => panic!("Expected bulk strings"),
            }
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_with_count_and_values() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    let result = ctx.hrandfield("myhash", Some(2), true).await.unwrap();
    match result {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4); // 2 fields * 2 (field + value)
            // Should be [field1, value1, field2, value2] or similar
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_negative_count() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    let result = ctx.hrandfield("myhash", Some(-3), false).await.unwrap();
    match result {
        RespValue::Array(fields) => {
            assert_eq!(fields.len(), 3); // Negative count allows duplicates
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_empty_hash() {
    let ctx = TestContext::new().await;

    let result = ctx.hrandfield("nonexistent", None, false).await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_hrandfield_empty_hash_with_count() {
    let ctx = TestContext::new().await;

    let result = ctx.hrandfield("nonexistent", Some(5), false).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_hrandfield_wrong_type() {
    let ctx = TestContext::new().await;

    ctx.set("mystring", "value").await.unwrap();

    let result = ctx.hrandfield("mystring", None, false).await;
    assert!(matches!(result, Err(SpinelDBError::WrongType)));
}

// ===== Complex Workflow Tests =====

#[tokio::test]
async fn test_hash_complex_workflow() {
    let ctx = TestContext::new().await;

    // Create hash with multiple fields
    ctx.hset(
        "user:1000",
        &[
            ("name", "Alice"),
            ("age", "30"),
            ("email", "alice@example.com"),
            ("score", "100"),
        ],
    )
    .await
    .unwrap();

    // Verify all fields exist
    assert_eq!(
        ctx.hget("user:1000", "name").await.unwrap(),
        RespValue::BulkString(Bytes::from("Alice"))
    );
    assert_eq!(
        ctx.hget("user:1000", "age").await.unwrap(),
        RespValue::BulkString(Bytes::from("30"))
    );

    // Update a field
    ctx.hset("user:1000", &[("age", "31")]).await.unwrap();

    // Increment score
    let result = ctx.hincrby("user:1000", "score", 10).await.unwrap();
    assert_eq!(result, RespValue::Integer(110));

    // Check length
    let result = ctx.hlen("user:1000").await.unwrap();
    assert_eq!(result, RespValue::Integer(4));

    // Get all keys
    let result = ctx.hkeys("user:1000").await.unwrap();
    match result {
        RespValue::Array(keys) => {
            assert_eq!(keys.len(), 4);
        }
        _ => panic!("Expected array"),
    }

    // Delete a field
    let result = ctx.hdel("user:1000", &["email"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify final state
    let result = ctx.hlen("user:1000").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_hash_type_error_consistency() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mystring", "value").await.unwrap();

    // All hash commands should return WrongType error
    assert!(matches!(
        ctx.hget("mystring", "field").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hset("mystring", &[("field", "value")]).await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hdel("mystring", &["field"]).await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hgetall("mystring").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hlen("mystring").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hkeys("mystring").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hvals("mystring").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hexists("mystring", "field").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hsetnx("mystring", "field", "value").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hincrby("mystring", "field", 1).await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hincrbyfloat("mystring", "field", 1.0).await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hstrlen("mystring", "field").await,
        Err(SpinelDBError::WrongType)
    ));
    assert!(matches!(
        ctx.hrandfield("mystring", None, false).await,
        Err(SpinelDBError::WrongType)
    ));
}

#[tokio::test]
async fn test_hash_empty_operations() {
    let ctx = TestContext::new().await;

    // All operations on empty/nonexistent hash should return appropriate empty values
    assert_eq!(
        ctx.hget("nonexistent", "field").await.unwrap(),
        RespValue::Null
    );
    assert_eq!(
        ctx.hlen("nonexistent").await.unwrap(),
        RespValue::Integer(0)
    );
    assert_eq!(
        ctx.hgetall("nonexistent").await.unwrap(),
        RespValue::Array(vec![])
    );
    assert_eq!(
        ctx.hkeys("nonexistent").await.unwrap(),
        RespValue::Array(vec![])
    );
    assert_eq!(
        ctx.hvals("nonexistent").await.unwrap(),
        RespValue::Array(vec![])
    );
    assert_eq!(
        ctx.hexists("nonexistent", "field").await.unwrap(),
        RespValue::Integer(0)
    );
    assert_eq!(
        ctx.hdel("nonexistent", &["field"]).await.unwrap(),
        RespValue::Integer(0)
    );
    assert_eq!(
        ctx.hstrlen("nonexistent", "field").await.unwrap(),
        RespValue::Integer(0)
    );
}

// ===== Additional Edge Case Tests for Coverage =====

#[tokio::test]
async fn test_hrandfield_count_zero() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // HRANDFIELD with count 0 should return empty array
    let result = ctx.hrandfield("myhash", Some(0), false).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_hrandfield_count_greater_than_size() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // HRANDFIELD with count > hash size should return all fields
    let result = ctx.hrandfield("myhash", Some(10), false).await.unwrap();
    match result {
        RespValue::Array(fields) => {
            assert_eq!(fields.len(), 2); // Should return all 2 fields
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_count_equal_to_size() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // HRANDFIELD with count = hash size should return all fields
    let result = ctx.hrandfield("myhash", Some(2), false).await.unwrap();
    match result {
        RespValue::Array(fields) => {
            assert_eq!(fields.len(), 2);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_withvalues_without_count() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // HRANDFIELD key WITHVALUES (without count) should return [field, value]
    let result = ctx.hrandfield("myhash", None, true).await.unwrap();
    match result {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            match (&items[0], &items[1]) {
                (RespValue::BulkString(field), RespValue::BulkString(value)) => {
                    let field_str = String::from_utf8(field.to_vec()).unwrap();
                    let value_str = String::from_utf8(value.to_vec()).unwrap();
                    assert!(field_str == "field1" || field_str == "field2");
                    assert!(value_str == "value1" || value_str == "value2");
                }
                _ => panic!("Expected bulk strings"),
            }
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_single_field_hash() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    // HRANDFIELD on single-field hash
    let result = ctx.hrandfield("myhash", None, false).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("field1")));

    // With count
    let result = ctx.hrandfield("myhash", Some(1), false).await.unwrap();
    match result {
        RespValue::Array(fields) => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0], RespValue::BulkString(Bytes::from("field1")));
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_negative_count_large() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // HRANDFIELD with large negative count (allows duplicates)
    let result = ctx.hrandfield("myhash", Some(-10), false).await.unwrap();
    match result {
        RespValue::Array(fields) => {
            assert_eq!(fields.len(), 10); // Should return 10 fields (with duplicates)
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_negative_count_with_values() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // HRANDFIELD with negative count and WITHVALUES
    let result = ctx.hrandfield("myhash", Some(-2), true).await.unwrap();
    match result {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4); // 2 fields * 2 (field + value)
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hincrby_underflow() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "-9223372036854775808")])
        .await
        .unwrap(); // i64::MIN

    // HINCRBY with -1 should cause underflow
    let result = ctx.hincrby("myhash", "field1", -1).await;
    assert!(matches!(result, Err(SpinelDBError::Overflow)));
}

#[tokio::test]
async fn test_hincrby_large_positive_increment() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "1000000000")])
        .await
        .unwrap();

    let result = ctx.hincrby("myhash", "field1", 1000000000).await.unwrap();
    assert_eq!(result, RespValue::Integer(2000000000));
}

#[tokio::test]
async fn test_hincrby_large_negative_increment() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "1000000000")])
        .await
        .unwrap();

    let result = ctx.hincrby("myhash", "field1", -500000000).await.unwrap();
    assert_eq!(result, RespValue::Integer(500000000));
}

#[tokio::test]
async fn test_hincrby_very_large_number() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "9223372036854775806")])
        .await
        .unwrap(); // i64::MAX - 1

    // Should succeed
    let result = ctx.hincrby("myhash", "field1", 1).await.unwrap();
    assert_eq!(result, RespValue::Integer(9223372036854775807)); // i64::MAX
}

#[tokio::test]
async fn test_hincrbyfloat_precision() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "0.1")]).await.unwrap();

    let result = ctx.hincrbyfloat("myhash", "field1", 0.2).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 0.3).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hincrbyfloat_zero_increment() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "10.5")]).await.unwrap();

    let result = ctx.hincrbyfloat("myhash", "field1", 0.0).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 10.5).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hincrbyfloat_very_small_increment() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "1.0")]).await.unwrap();

    let result = ctx
        .hincrbyfloat("myhash", "field1", 0.0000001)
        .await
        .unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 1.0000001).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hincrbyfloat_negative_float_field() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "-10.5")]).await.unwrap();

    let result = ctx.hincrbyfloat("myhash", "field1", 5.5).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - (-5.0)).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hset_empty_fields_error() {
    let ctx = TestContext::new().await;

    // HSET with empty field-value pairs should error
    // Note: This might be caught at parse time, but let's test execution path
    let result = ctx.hset("myhash", &[]).await;
    // Should either error at parse or execution
    assert!(result.is_err());
}

#[tokio::test]
async fn test_hmget_empty_fields_error() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    // HMGET with empty fields should error at parse time
    let result = ctx.hmget("myhash", &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_hset_single_field_update_memory() {
    let ctx = TestContext::new().await;

    // Test that updating a field with different size values works
    ctx.hset("myhash", &[("field1", "short")]).await.unwrap();
    let result = ctx.hget("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("short")));

    // Update with longer value
    ctx.hset("myhash", &[("field1", "much_longer_value")])
        .await
        .unwrap();
    let result = ctx.hget("myhash", "field1").await.unwrap();
    assert_eq!(
        result,
        RespValue::BulkString(Bytes::from("much_longer_value"))
    );

    // Update with shorter value
    ctx.hset("myhash", &[("field1", "x")]).await.unwrap();
    let result = ctx.hget("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("x")));
}

#[tokio::test]
async fn test_hdel_all_fields() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    // Delete all fields
    let result = ctx
        .hdel("myhash", &["field1", "field2", "field3"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Hash should be empty
    let result = ctx.hlen("myhash").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hdel_partial_nonexistent() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1"), ("field2", "value2")])
        .await
        .unwrap();

    // Delete mix of existing and nonexistent fields
    let result = ctx
        .hdel("myhash", &["field1", "nonexistent", "field2"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Only 2 fields deleted

    // Hash should be empty
    let result = ctx.hlen("myhash").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_hgetall_after_multiple_updates() {
    let ctx = TestContext::new().await;

    // Create hash
    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    // Update field1 multiple times
    ctx.hset("myhash", &[("field1", "value1_updated")])
        .await
        .unwrap();
    ctx.hset("myhash", &[("field1", "value1_final")])
        .await
        .unwrap();

    // Add more fields
    ctx.hset("myhash", &[("field2", "value2"), ("field3", "value3")])
        .await
        .unwrap();

    let result = ctx.hgetall("myhash").await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 6); // 3 fields * 2 (field + value)
            // Verify field1 has final value
            let field1_idx = values
                .iter()
                .position(|v| {
                    if let RespValue::BulkString(b) = v {
                        b == &Bytes::from("field1")
                    } else {
                        false
                    }
                })
                .unwrap();
            assert_eq!(
                values[field1_idx + 1],
                RespValue::BulkString(Bytes::from("value1_final"))
            );
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hsetnx_on_new_hash() {
    let ctx = TestContext::new().await;

    // HSETNX on new hash should create it
    let result = ctx.hsetnx("newhash", "field1", "value1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify hash exists
    let result = ctx.hlen("newhash").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_hsetnx_multiple_fields() {
    let ctx = TestContext::new().await;

    // Create hash with one field
    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    // HSETNX on existing field should fail
    let result = ctx.hsetnx("myhash", "field1", "value2").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // HSETNX on new field should succeed
    let result = ctx.hsetnx("myhash", "field2", "value2").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify both fields
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("value1"))
    );
    assert_eq!(
        ctx.hget("myhash", "field2").await.unwrap(),
        RespValue::BulkString(Bytes::from("value2"))
    );
}

#[tokio::test]
async fn test_hstrlen_unicode() {
    let ctx = TestContext::new().await;

    // Test with unicode characters (UTF-8)
    ctx.hset("myhash", &[("field1", "café")]).await.unwrap();

    let result = ctx.hstrlen("myhash", "field1").await.unwrap();
    // "café" is 5 bytes in UTF-8 (c-a-f-é where é is 2 bytes)
    assert_eq!(result, RespValue::Integer(5));
}

#[tokio::test]
async fn test_hstrlen_very_long_string() {
    let ctx = TestContext::new().await;

    let long_value = "a".repeat(1000);
    ctx.hset("myhash", &[("field1", &long_value)])
        .await
        .unwrap();

    let result = ctx.hstrlen("myhash", "field1").await.unwrap();
    assert_eq!(result, RespValue::Integer(1000));
}

#[tokio::test]
async fn test_hincrby_new_field_zero() {
    let ctx = TestContext::new().await;

    // HINCRBY on new field should start at 0
    let result = ctx.hincrby("myhash", "field1", 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("0"))
    );
}

#[tokio::test]
async fn test_hincrby_chain_operations() {
    let ctx = TestContext::new().await;

    // Chain multiple HINCRBY operations
    let result = ctx.hincrby("myhash", "field1", 5).await.unwrap();
    assert_eq!(result, RespValue::Integer(5));

    let result = ctx.hincrby("myhash", "field1", 10).await.unwrap();
    assert_eq!(result, RespValue::Integer(15));

    let result = ctx.hincrby("myhash", "field1", -5).await.unwrap();
    assert_eq!(result, RespValue::Integer(10));

    // Verify final value
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("10"))
    );
}

#[tokio::test]
async fn test_hincrbyfloat_chain_operations() {
    let ctx = TestContext::new().await;

    // Chain multiple HINCRBYFLOAT operations
    let result = ctx.hincrbyfloat("myhash", "field1", 1.5).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 1.5).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }

    let result = ctx.hincrbyfloat("myhash", "field1", 2.5).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 4.0).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_hkeys_after_deletion() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    // Delete one field
    ctx.hdel("myhash", &["field2"]).await.unwrap();

    // HKEYS should return remaining fields
    let result = ctx.hkeys("myhash").await.unwrap();
    match result {
        RespValue::Array(keys) => {
            assert_eq!(keys.len(), 2);
            // Should not contain field2
            let has_field2 = keys.iter().any(|k| {
                if let RespValue::BulkString(b) = k {
                    b == &Bytes::from("field2")
                } else {
                    false
                }
            });
            assert!(!has_field2);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hvals_after_deletion() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    // Delete one field
    ctx.hdel("myhash", &["field2"]).await.unwrap();

    // HVALS should return remaining values
    let result = ctx.hvals("myhash").await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            // Should not contain value2
            let has_value2 = values.iter().any(|v| {
                if let RespValue::BulkString(b) = v {
                    b == &Bytes::from("value2")
                } else {
                    false
                }
            });
            assert!(!has_value2);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hmget_all_nonexistent_fields() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    // HMGET with all nonexistent fields
    let result = ctx
        .hmget("myhash", &["nonexistent1", "nonexistent2"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::Null);
            assert_eq!(values[1], RespValue::Null);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hexists_after_deletion() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "value1")]).await.unwrap();

    // Field should exist
    assert_eq!(
        ctx.hexists("myhash", "field1").await.unwrap(),
        RespValue::Integer(1)
    );

    // Delete field
    ctx.hdel("myhash", &["field1"]).await.unwrap();

    // Field should not exist
    assert_eq!(
        ctx.hexists("myhash", "field1").await.unwrap(),
        RespValue::Integer(0)
    );
}

#[tokio::test]
async fn test_hset_multiple_updates_same_field() {
    let ctx = TestContext::new().await;

    // Update same field multiple times in one HSET
    let result = ctx
        .hset("myhash", &[("field1", "value1"), ("field1", "value2")])
        .await
        .unwrap();
    // Should return 1 (one new field, one update)
    assert_eq!(result, RespValue::Integer(1));

    // Final value should be the last one
    assert_eq!(
        ctx.hget("myhash", "field1").await.unwrap(),
        RespValue::BulkString(Bytes::from("value2"))
    );
}

#[tokio::test]
async fn test_hgetall_large_hash() {
    let ctx = TestContext::new().await;

    // Create a hash with many fields
    let mut field_values = Vec::new();
    for i in 0..20 {
        field_values.push((format!("field{}", i), format!("value{}", i)));
    }
    let field_values_refs: Vec<(&str, &str)> = field_values
        .iter()
        .map(|(f, v)| (f.as_str(), v.as_str()))
        .collect();

    ctx.hset("myhash", &field_values_refs).await.unwrap();

    let result = ctx.hgetall("myhash").await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 40); // 20 fields * 2
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hrandfield_count_one() {
    let ctx = TestContext::new().await;

    ctx.hset(
        "myhash",
        &[
            ("field1", "value1"),
            ("field2", "value2"),
            ("field3", "value3"),
        ],
    )
    .await
    .unwrap();

    // HRANDFIELD with count 1
    let result = ctx.hrandfield("myhash", Some(1), false).await.unwrap();
    match result {
        RespValue::Array(fields) => {
            assert_eq!(fields.len(), 1);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_hincrby_negative_starting_value() {
    let ctx = TestContext::new().await;

    ctx.hset("myhash", &[("field1", "-10")]).await.unwrap();

    let result = ctx.hincrby("myhash", "field1", 5).await.unwrap();
    assert_eq!(result, RespValue::Integer(-5));
}

#[tokio::test]
async fn test_hincrbyfloat_scientific_notation() {
    let ctx = TestContext::new().await;

    // Test that HINCRBYFLOAT can handle values that might be in scientific notation
    ctx.hset("myhash", &[("field1", "1e2")]).await.unwrap(); // 100

    let result = ctx.hincrbyfloat("myhash", "field1", 50.0).await.unwrap();
    match result {
        RespValue::BulkString(value) => {
            let val_str = String::from_utf8(value.to_vec()).unwrap();
            let val: f64 = val_str.parse().unwrap();
            assert!((val - 150.0).abs() < 0.0001);
        }
        _ => panic!("Expected bulk string"),
    }
}
