// tests/integration/string_commands_test.rs

//! Integration tests for string commands
//! Tests: SET, GET, DEL, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, etc.

use super::fixtures::constants;
use super::fixtures::*;
use super::test_helpers::TestContext;
use bytes::Bytes;
use spineldb::core::Command;
use spineldb::core::RespValue;
use spineldb::core::protocol::RespFrame;

// ===== Basic SET/GET Tests =====

#[tokio::test]
async fn test_set_get_basic() {
    let ctx = TestContext::new().await;

    // SET a key
    let result = ctx.set("mykey", "myvalue").await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // GET the key
    let result = ctx.get("mykey").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("myvalue")));
}

#[tokio::test]
async fn test_get_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.get("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_set_overwrite() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set(TEST_KEY1, TEST_VALUE1).await.unwrap();

    // Overwrite with new value
    ctx.set(TEST_KEY1, TEST_VALUE2).await.unwrap();

    // Verify new value
    let result = ctx.get(TEST_KEY1).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from(TEST_VALUE2)));
}

#[tokio::test]
async fn test_set_get_empty_string() {
    let ctx = TestContext::new().await;

    ctx.set("empty_key", patterns::EMPTY_STR).await.unwrap();
    let result = ctx.get("empty_key").await.unwrap();
    assert_eq!(
        result,
        RespValue::BulkString(Bytes::from(patterns::EMPTY_STR))
    );
}

#[tokio::test]
async fn test_set_get_unicode() {
    let ctx = TestContext::new().await;

    let unicode_value = patterns::UNICODE_STR;
    ctx.set("unicode_key", unicode_value).await.unwrap();

    let result = ctx.get("unicode_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from(unicode_value)));
}

#[tokio::test]
async fn test_set_get_binary_data() {
    let ctx = TestContext::new().await;

    // Binary data with null bytes
    let binary_data = vec![0x00, 0x01, 0xFF, 0x00, 0xAB];

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("binary_key")),
        RespFrame::BulkString(Bytes::from(binary_data.clone())),
    ]))
    .unwrap();

    ctx.execute(command).await.unwrap();

    let result = ctx.get("binary_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from(binary_data)));
}

#[tokio::test]
async fn test_set_get_large_value() {
    let ctx = TestContext::new().await;

    let large_value = patterns::large_text_1kb();
    ctx.set("large_key", &large_value).await.unwrap();

    let result = ctx.get("large_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from(large_value)));
}

// ===== SET with Options Tests =====

#[tokio::test]
async fn test_set_nx_success() {
    let ctx = TestContext::new().await;

    // SET NX should succeed if key doesn't exist
    let result = ctx.set_nx("nx_key", "value").await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify value was set
    let result = ctx.get("nx_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_set_nx_failure() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set("existing_key", "initial").await.unwrap();

    // SET NX should fail if key exists
    let result = ctx.set_nx("existing_key", "new_value").await.unwrap();
    assert_eq!(result, RespValue::Null);

    // Verify original value unchanged
    let result = ctx.get("existing_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("initial")));
}

#[tokio::test]
async fn test_set_xx_success() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set("xx_key", "initial").await.unwrap();

    // SET XX should succeed if key exists
    let result = ctx.set_xx("xx_key", "updated").await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify value was updated
    let result = ctx.get("xx_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("updated")));
}

#[tokio::test]
async fn test_set_xx_failure() {
    let ctx = TestContext::new().await;

    // SET XX should fail if key doesn't exist
    let result = ctx.set_xx("nonexistent", "value").await.unwrap();
    assert_eq!(result, RespValue::Null);

    // Verify key was not created
    let result = ctx.get("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_set_with_get() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set("get_key", "old_value").await.unwrap();

    // SET with GET option should return old value
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("get_key")),
        RespFrame::BulkString(Bytes::from("new_value")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("old_value")));

    // Verify new value was set
    let result = ctx.get("get_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("new_value")));
}

// ===== DEL Tests =====

#[tokio::test]
async fn test_del_single_key() {
    let ctx = TestContext::new().await;

    // Set a key
    ctx.set("del_key", "value").await.unwrap();

    // Delete the key
    let result = ctx.del(&["del_key"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify key is gone
    let result = ctx.get("del_key").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_del_multiple_keys() {
    let ctx = TestContext::new().await;

    // Set multiple keys using fixtures
    ctx.set(TEST_KEY1, TEST_VALUE1).await.unwrap();
    ctx.set(TEST_KEY2, TEST_VALUE2).await.unwrap();
    ctx.set(TEST_KEY3, TEST_VALUE3).await.unwrap();

    // Delete multiple keys
    let result = ctx.del(&[TEST_KEY1, TEST_KEY2, TEST_KEY3]).await.unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify all keys are gone
    let result = ctx.get(TEST_KEY1).await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_del_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.del(&["nonexistent"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_del_mixed_existing_nonexistent() {
    let ctx = TestContext::new().await;

    // Set one key
    ctx.set("exists", "value").await.unwrap();

    // Delete mix of existing and non-existing
    let result = ctx
        .del(&["exists", "not_exists", "also_not_exists"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

// ===== EXISTS Tests =====

#[tokio::test]
async fn test_exists_single_key() {
    let ctx = TestContext::new().await;

    // Key doesn't exist
    let result = ctx.exists(&["test_key"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Set the key
    ctx.set("test_key", "value").await.unwrap();

    // Key exists
    let result = ctx.exists(&["test_key"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_exists_multiple_keys() {
    let ctx = TestContext::new().await;

    ctx.set(TEST_KEY1, TEST_VALUE1).await.unwrap();
    ctx.set(TEST_KEY2, TEST_VALUE2).await.unwrap();

    // Count existing keys
    let result = ctx
        .exists(&[TEST_KEY1, TEST_KEY2, TEST_KEY3])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

// ===== APPEND Tests =====

#[tokio::test]
async fn test_append_to_existing_key() {
    let ctx = TestContext::new().await;

    ctx.set("append_key", "Hello").await.unwrap();

    let result = ctx.append("append_key", " World").await.unwrap();
    assert_eq!(result, RespValue::Integer(11)); // Length of "Hello World"

    // Verify appended value
    let result = ctx.get("append_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("Hello World")));
}

#[tokio::test]
async fn test_append_to_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.append("new_key", "value").await.unwrap();
    assert_eq!(result, RespValue::Integer(5)); // Length of "value"

    // Verify value was set
    let result = ctx.get("new_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

// ===== STRLEN Tests =====

#[tokio::test]
async fn test_strlen_existing_key() {
    let ctx = TestContext::new().await;

    ctx.set("strlen_key", "Hello").await.unwrap();

    let result = ctx.strlen("strlen_key").await.unwrap();
    assert_eq!(result, RespValue::Integer(5));
}

#[tokio::test]
async fn test_strlen_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.strlen("nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_strlen_empty_string() {
    let ctx = TestContext::new().await;

    ctx.set("empty", "").await.unwrap();

    let result = ctx.strlen("empty").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

// ===== GETRANGE Tests =====

#[tokio::test]
async fn test_getrange_basic() {
    let ctx = TestContext::new().await;

    ctx.set("range_key", "Hello World").await.unwrap();

    let result = ctx.getrange("range_key", 0, 4).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("Hello")));
}

#[tokio::test]
async fn test_getrange_negative_indices() {
    let ctx = TestContext::new().await;

    ctx.set("range_key", "Hello World").await.unwrap();

    // Get last 5 characters
    let result = ctx.getrange("range_key", -5, -1).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("World")));
}

// ===== SETRANGE Tests =====

#[tokio::test]
async fn test_setrange_existing_key() {
    let ctx = TestContext::new().await;

    ctx.set("setrange_key", "Hello World").await.unwrap();

    ctx.setrange("setrange_key", 6, "Redis").await.unwrap();

    // Verify modified value
    let result = ctx.get("setrange_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("Hello Redis")));
}

// ===== INCR/DECR Tests =====

#[tokio::test]
async fn test_incr_basic() {
    let ctx = TestContext::new().await;

    ctx.set("counter", "10").await.unwrap();

    let result = ctx.incr("counter").await.unwrap();
    assert_eq!(result, RespValue::Integer(11));

    // Verify value
    let result = ctx.get("counter").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("11")));
}

#[tokio::test]
async fn test_incr_nonexistent_key() {
    let ctx = TestContext::new().await;

    let result = ctx.incr("new_counter").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_decr_basic() {
    let ctx = TestContext::new().await;

    ctx.set("counter", "10").await.unwrap();

    let result = ctx.decr("counter").await.unwrap();
    assert_eq!(result, RespValue::Integer(9));
}

#[tokio::test]
async fn test_incrby() {
    let ctx = TestContext::new().await;

    ctx.set("counter", "10").await.unwrap();

    let result = ctx.incrby("counter", 5).await.unwrap();
    assert_eq!(result, RespValue::Integer(15));
}

#[tokio::test]
async fn test_decrby() {
    let ctx = TestContext::new().await;

    ctx.set("counter", "10").await.unwrap();

    let result = ctx.decrby("counter", 3).await.unwrap();
    assert_eq!(result, RespValue::Integer(7));
}

// ===== MGET Tests =====

#[tokio::test]
async fn test_mget_multiple_keys() {
    let ctx = TestContext::new().await;

    ctx.set(TEST_KEY1, TEST_VALUE1).await.unwrap();
    ctx.set(TEST_KEY2, TEST_VALUE2).await.unwrap();

    let result = ctx.mget(&[TEST_KEY1, TEST_KEY2, TEST_KEY3]).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Bytes::from(TEST_VALUE1)));
            assert_eq!(values[1], RespValue::BulkString(Bytes::from(TEST_VALUE2)));
            assert_eq!(values[2], RespValue::Null);
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_mget_empty_key_list() {
    let ctx = TestContext::new().await;

    // MGET with empty key list should return error (WrongArgumentCount)
    let result = ctx.mget(&[]).await;
    assert!(
        result.is_err(),
        "MGET with empty key list should return error"
    );
}

// ===== MSET Tests =====

#[tokio::test]
async fn test_mset_multiple_keys() {
    let ctx = TestContext::new().await;

    let result = ctx
        .mset(&[(TEST_KEY1, TEST_VALUE1), (TEST_KEY2, TEST_VALUE2)])
        .await
        .unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify values
    let result = ctx.get(TEST_KEY1).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from(TEST_VALUE1)));

    let result = ctx.get(TEST_KEY2).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from(TEST_VALUE2)));
}

#[tokio::test]
async fn test_mset_odd_number_of_arguments() {
    // MSET with odd number of arguments (key without value) should fail
    // This should fail at command parsing level
    let command_result = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"MSET")),
        RespFrame::BulkString(Bytes::from(TEST_KEY1)),
        RespFrame::BulkString(Bytes::from(TEST_VALUE1)),
        RespFrame::BulkString(Bytes::from(TEST_KEY2)),
        // Missing value for TEST_KEY2
    ]));

    assert!(
        command_result.is_err(),
        "MSET with odd number of arguments should fail at parse time"
    );
}

// ===== Concurrency Tests =====

#[tokio::test]
async fn test_concurrent_set_get() {
    let ctx = TestContext::new().await;

    // Set multiple keys sequentially using unique_key fixture
    for i in 0..10 {
        let key = unique_key("concurrent", i);
        let value = format!("value_{}", i);

        ctx.set(&key, &value).await.unwrap();
    }

    // Verify all keys
    for i in 0..10 {
        let key = unique_key("concurrent", i);
        let expected = format!("value_{}", i);

        let result = ctx.get(&key).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(expected)));
    }
}

// ===== SET with TTL Tests =====

#[tokio::test]
async fn test_set_with_ex() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("ttl_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"EX")),
        RespFrame::BulkString(Bytes::from(constants::DEFAULT_TTL_SECONDS.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify value exists
    let result = ctx.get("ttl_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));

    // Verify TTL was set (should be around 60 seconds, allow some tolerance)
    let ttl_result = ctx.ttl("ttl_key").await.unwrap();
    match ttl_result {
        RespValue::Integer(ttl) => {
            assert!(
                ttl > 50 && ttl <= 60,
                "TTL should be between 50 and 60, got {}",
                ttl
            );
        }
        _ => panic!("Expected integer TTL"),
    }
}

#[tokio::test]
async fn test_set_with_px() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("px_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"PX")),
        RespFrame::BulkString(Bytes::from(constants::DEFAULT_TTL_MILLIS.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify value exists
    let result = ctx.get("px_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));

    // Verify TTL was set (should be around 60 seconds, allow some tolerance)
    let ttl_result = ctx.ttl("px_key").await.unwrap();
    match ttl_result {
        RespValue::Integer(ttl) => {
            assert!(
                ttl > 50 && ttl <= 60,
                "TTL should be between 50 and 60, got {}",
                ttl
            );
        }
        _ => panic!("Expected integer TTL"),
    }
}

// ===== SETEX Tests =====

#[tokio::test]
async fn test_setex_basic() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETEX")),
        RespFrame::BulkString(Bytes::from("setex_key")),
        RespFrame::BulkString(Bytes::from("30")),
        RespFrame::BulkString(Bytes::from("value")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify value
    let result = ctx.get("setex_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));

    // Verify TTL was set (should be around 30 seconds, allow some tolerance)
    let ttl_result = ctx.ttl("setex_key").await.unwrap();
    match ttl_result {
        RespValue::Integer(ttl) => {
            assert!(
                ttl > 20 && ttl <= 30,
                "TTL should be between 20 and 30, got {}",
                ttl
            );
        }
        _ => panic!("Expected integer TTL"),
    }
}

#[tokio::test]
async fn test_setex_overwrite() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set("setex_key", "old_value").await.unwrap();

    // Overwrite with SETEX
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETEX")),
        RespFrame::BulkString(Bytes::from("setex_key")),
        RespFrame::BulkString(Bytes::from("60")),
        RespFrame::BulkString(Bytes::from("new_value")),
    ]))
    .unwrap();

    ctx.execute(command).await.unwrap();

    // Verify new value
    let result = ctx.get("setex_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("new_value")));
}

// ===== PSETEX Tests =====

#[tokio::test]
async fn test_psetex_basic() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"PSETEX")),
        RespFrame::BulkString(Bytes::from("psetex_key")),
        RespFrame::BulkString(Bytes::from("30000")),
        RespFrame::BulkString(Bytes::from("value")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify value
    let result = ctx.get("psetex_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));

    // Verify TTL was set (should be around 30 seconds, allow some tolerance)
    let ttl_result = ctx.ttl("psetex_key").await.unwrap();
    match ttl_result {
        RespValue::Integer(ttl) => {
            assert!(
                ttl > 20 && ttl <= 30,
                "TTL should be between 20 and 30, got {}",
                ttl
            );
        }
        _ => panic!("Expected integer TTL"),
    }
}

// ===== GETEX Tests =====

#[tokio::test]
async fn test_getex_basic() {
    let ctx = TestContext::new().await;

    // Set a value first
    ctx.set("getex_key", "value").await.unwrap();

    // GETEX without options should just return the value
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("getex_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_getex_with_ex() {
    let ctx = TestContext::new().await;

    ctx.set("getex_key", "value").await.unwrap();

    // GETEX with EX option
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("getex_key")),
        RespFrame::BulkString(Bytes::from_static(b"EX")),
        RespFrame::BulkString(Bytes::from(constants::DEFAULT_TTL_SECONDS.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));

    // Verify value still exists
    let result = ctx.get("getex_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_getex_with_px() {
    let ctx = TestContext::new().await;

    ctx.set("getex_key", "value").await.unwrap();

    // GETEX with PX option
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("getex_key")),
        RespFrame::BulkString(Bytes::from_static(b"PX")),
        RespFrame::BulkString(Bytes::from(constants::DEFAULT_TTL_MILLIS.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_getex_with_persist() {
    let ctx = TestContext::new().await;

    // Set a value with TTL first
    let set_cmd = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("getex_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"EX")),
        RespFrame::BulkString(Bytes::from(constants::DEFAULT_TTL_SECONDS.to_string())),
    ]))
    .unwrap();
    ctx.execute(set_cmd).await.unwrap();

    // GETEX with PERSIST to remove TTL
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("getex_key")),
        RespFrame::BulkString(Bytes::from_static(b"PERSIST")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_getex_nonexistent() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("nonexistent")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Null);
}

// ===== GETSET Tests =====

#[tokio::test]
async fn test_getset_existing_key() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set("getset_key", "old_value").await.unwrap();

    // GETSET should return old value and set new value
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETSET")),
        RespFrame::BulkString(Bytes::from("getset_key")),
        RespFrame::BulkString(Bytes::from("new_value")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("old_value")));

    // Verify new value was set
    let result = ctx.get("getset_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("new_value")));
}

#[tokio::test]
async fn test_getset_nonexistent_key() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETSET")),
        RespFrame::BulkString(Bytes::from("new_key")),
        RespFrame::BulkString(Bytes::from("value")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Null);

    // Verify value was set
    let result = ctx.get("new_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

// ===== GETDEL Tests =====

#[tokio::test]
async fn test_getdel_existing_key() {
    let ctx = TestContext::new().await;

    ctx.set("getdel_key", "value").await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETDEL")),
        RespFrame::BulkString(Bytes::from("getdel_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));

    // Verify key was deleted
    let result = ctx.get("getdel_key").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_getdel_nonexistent_key() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETDEL")),
        RespFrame::BulkString(Bytes::from("nonexistent")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Null);
}

// ===== MSETNX Tests =====

#[tokio::test]
async fn test_msetnx_all_new_keys() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"MSETNX")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("value1")),
        RespFrame::BulkString(Bytes::from("key2")),
        RespFrame::BulkString(Bytes::from("value2")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(1)); // All keys were set

    // Verify both keys
    assert_eq!(
        ctx.get("key1").await.unwrap(),
        RespValue::BulkString(Bytes::from("value1"))
    );
    assert_eq!(
        ctx.get("key2").await.unwrap(),
        RespValue::BulkString(Bytes::from("value2"))
    );
}

#[tokio::test]
async fn test_msetnx_with_existing_key() {
    let ctx = TestContext::new().await;

    // Set one key first
    ctx.set("existing_key", "old_value").await.unwrap();

    // MSETNX with one existing and one new key should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"MSETNX")),
        RespFrame::BulkString(Bytes::from("existing_key")),
        RespFrame::BulkString(Bytes::from("new_value")),
        RespFrame::BulkString(Bytes::from("new_key")),
        RespFrame::BulkString(Bytes::from("value")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0)); // Failed because one key exists

    // Verify existing key unchanged
    assert_eq!(
        ctx.get("existing_key").await.unwrap(),
        RespValue::BulkString(Bytes::from("old_value"))
    );

    // Verify new key was NOT set
    assert_eq!(ctx.get("new_key").await.unwrap(), RespValue::Null);
}

// ===== INCRBYFLOAT Tests =====

#[tokio::test]
async fn test_incrbyfloat_existing_key() {
    let ctx = TestContext::new().await;

    ctx.set("float_key", "10.5").await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INCRBYFLOAT")),
        RespFrame::BulkString(Bytes::from("float_key")),
        RespFrame::BulkString(Bytes::from("2.5")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    // Result should be a string representation of 13.0
    match result {
        RespValue::BulkString(val) => {
            let val_str = String::from_utf8_lossy(&val);
            let parsed: f64 = val_str.parse().unwrap();
            assert!((parsed - 13.0).abs() < 0.001);
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_incrbyfloat_nonexistent_key() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INCRBYFLOAT")),
        RespFrame::BulkString(Bytes::from("new_float_key")),
        RespFrame::BulkString(Bytes::from("5.5")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::BulkString(val) => {
            let val_str = String::from_utf8_lossy(&val);
            let parsed: f64 = val_str.parse().unwrap();
            assert!((parsed - 5.5).abs() < 0.001);
        }
        _ => panic!("Expected BulkString"),
    }

    // Verify value was set
    let result = ctx.get("new_float_key").await.unwrap();
    match result {
        RespValue::BulkString(val) => {
            let val_str = String::from_utf8_lossy(&val);
            let parsed: f64 = val_str.parse().unwrap();
            assert!((parsed - 5.5).abs() < 0.001);
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_incrbyfloat_negative() {
    let ctx = TestContext::new().await;

    ctx.set("float_key", "10.0").await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INCRBYFLOAT")),
        RespFrame::BulkString(Bytes::from("float_key")),
        RespFrame::BulkString(Bytes::from("-3.5")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::BulkString(val) => {
            let val_str = String::from_utf8_lossy(&val);
            let parsed: f64 = val_str.parse().unwrap();
            assert!((parsed - 6.5).abs() < 0.001);
        }
        _ => panic!("Expected BulkString"),
    }
}

// ===== Error Path Tests =====

#[tokio::test]
async fn test_incr_on_non_numeric_string() {
    let ctx = TestContext::new().await;

    ctx.set("non_numeric", "not_a_number").await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INCR")),
        RespFrame::BulkString(Bytes::from("non_numeric")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
    // Should return a numeric error
}

#[tokio::test]
async fn test_incrby_on_non_numeric_string() {
    let ctx = TestContext::new().await;

    ctx.set("non_numeric", "not_a_number").await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INCRBY")),
        RespFrame::BulkString(Bytes::from("non_numeric")),
        RespFrame::BulkString(Bytes::from("5")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_incrbyfloat_on_non_numeric_string() {
    let ctx = TestContext::new().await;

    ctx.set("non_numeric", "not_a_number").await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INCRBYFLOAT")),
        RespFrame::BulkString(Bytes::from("non_numeric")),
        RespFrame::BulkString(Bytes::from("1.5")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_append_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list (non-string type)
    ctx.create_list("list_key", "item1").await.unwrap();

    // Try to APPEND to a list (should fail with WrongType)
    let result = ctx.append("list_key", "value").await;
    assert!(result.is_err());
    // Should return WrongType error
}

#[tokio::test]
async fn test_getset_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "item1").await.unwrap();

    // Try GETSET on a list (should fail)
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETSET")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("value")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_getdel_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "item1").await.unwrap();

    // Try GETDEL on a list (should fail)
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETDEL")),
        RespFrame::BulkString(Bytes::from("list_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

// ===== Edge Cases for INCR/DECR =====

#[tokio::test]
async fn test_incr_large_number() {
    let ctx = TestContext::new().await;

    ctx.set("large_num", constants::NEAR_I64_MAX).await.unwrap();

    let result = ctx.incr("large_num").await.unwrap();
    match result {
        RespValue::Integer(val) => {
            assert_eq!(val, constants::I64_MAX); // Should be exactly i64::MAX
        }
        _ => panic!("Expected Integer"),
    }

    // Verify value was updated
    let result = ctx.get("large_num").await.unwrap();
    match result {
        RespValue::BulkString(bytes) => {
            let val_str = String::from_utf8_lossy(&bytes);
            let parsed: i64 = val_str.parse().unwrap();
            assert_eq!(parsed, constants::I64_MAX);
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_decr_to_zero() {
    let ctx = TestContext::new().await;

    ctx.set("counter", "1").await.unwrap();

    let result = ctx.decr("counter").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify value was updated
    let result = ctx.get("counter").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("0")));
}

#[tokio::test]
async fn test_decr_negative() {
    let ctx = TestContext::new().await;

    ctx.set("counter", "0").await.unwrap();

    let result = ctx.decr("counter").await.unwrap();
    assert_eq!(result, RespValue::Integer(-1));

    // Verify value was updated
    let result = ctx.get("counter").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("-1")));
}

// ===== More SET Edge Cases =====

#[tokio::test]
async fn test_set_with_exat() {
    let ctx = TestContext::new().await;

    // Set with EXAT (Unix timestamp in seconds)
    let future_timestamp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs())
        + 60;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("exat_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"EXAT")),
        RespFrame::BulkString(Bytes::from(future_timestamp.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));
}

#[tokio::test]
async fn test_set_with_pxat() {
    let ctx = TestContext::new().await;

    // Set with PXAT (Unix timestamp in milliseconds)
    let future_timestamp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis())
        + 60000;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("pxat_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"PXAT")),
        RespFrame::BulkString(Bytes::from(future_timestamp.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));
}

#[tokio::test]
async fn test_set_with_kepttl() {
    let ctx = TestContext::new().await;

    // First set with TTL
    let set_cmd = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("kepttl_key")),
        RespFrame::BulkString(Bytes::from("old_value")),
        RespFrame::BulkString(Bytes::from_static(b"EX")),
        RespFrame::BulkString(Bytes::from(constants::DEFAULT_TTL_SECONDS.to_string())),
    ]))
    .unwrap();
    ctx.execute(set_cmd).await.unwrap();

    // Update value but keep TTL
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("kepttl_key")),
        RespFrame::BulkString(Bytes::from("new_value")),
        RespFrame::BulkString(Bytes::from_static(b"KEEPTTL")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify new value
    let result = ctx.get("kepttl_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("new_value")));
}

// ===== GETRANGE Edge Cases =====

#[tokio::test]
async fn test_getrange_start_greater_than_end() {
    let ctx = TestContext::new().await;

    ctx.set("range_key", "Hello").await.unwrap();

    // Start > end should return empty string
    let result = ctx.getrange("range_key", 3, 1).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("")));
}

#[tokio::test]
async fn test_getrange_out_of_bounds() {
    let ctx = TestContext::new().await;

    ctx.set("range_key", "Hello").await.unwrap();

    // Start beyond string length
    let result = ctx.getrange("range_key", 100, 200).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("")));
}

#[tokio::test]
async fn test_getrange_same_start_end() {
    let ctx = TestContext::new().await;

    ctx.set("range_key", "Hello").await.unwrap();

    // Start == end should return single character
    let result = ctx.getrange("range_key", 1, 1).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("e")));
}

// ===== SETRANGE Edge Cases =====

#[tokio::test]
async fn test_setrange_beyond_string_length() {
    let ctx = TestContext::new().await;

    ctx.set("setrange_key", "Hello").await.unwrap();

    // SETRANGE beyond string length should pad with null bytes
    ctx.setrange("setrange_key", 10, "World").await.unwrap();

    // String should be padded: "Hello\0\0\0\0\0World" (5 null bytes between)
    let result = ctx.get("setrange_key").await.unwrap();
    match result {
        RespValue::BulkString(bytes) => {
            assert_eq!(bytes.len(), 15); // "Hello" (5) + 5 null bytes + "World" (5)
            assert_eq!(&bytes[0..5], b"Hello"); // Verify prefix
            assert_eq!(&bytes[10..15], b"World"); // Verify suffix
            // Verify null bytes in between (indices 5-9)
            for i in 5..10 {
                assert_eq!(bytes[i], 0);
            }
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_setrange_at_end() {
    let ctx = TestContext::new().await;

    ctx.set("setrange_key", "Hello").await.unwrap();

    // SETRANGE at the end
    ctx.setrange("setrange_key", 5, " World").await.unwrap();

    let result = ctx.get("setrange_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("Hello World")));
}

#[tokio::test]
async fn test_setrange_with_empty_string() {
    let ctx = TestContext::new().await;

    ctx.set("setrange_key", "Hello World").await.unwrap();

    // SETRANGE with empty string should not modify the string
    // (Redis behavior: empty string means no replacement)
    ctx.setrange("setrange_key", 5, "").await.unwrap();

    let result = ctx.get("setrange_key").await.unwrap();
    // String should remain unchanged
    assert_eq!(result, RespValue::BulkString(Bytes::from("Hello World")));
}

// ===== Additional SET Tests for Coverage =====

#[tokio::test]
async fn test_set_with_get_on_nonexistent() {
    let ctx = TestContext::new().await;

    // SET with GET on non-existent key should return Null
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("new_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_set_with_get_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "item1").await.unwrap();

    // SET with GET on non-string type should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

// Note: PERSIST is not a valid SET option - it's a separate command
// Removing this test as SET doesn't accept PERSIST as an option

#[tokio::test]
async fn test_set_nx_with_get() {
    let ctx = TestContext::new().await;

    // SET NX GET on non-existent key
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("nx_get_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"NX")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Null); // No old value

    // Verify value was set
    let result = ctx.get("nx_get_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_set_xx_with_get() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set("xx_get_key", "old_value").await.unwrap();

    // SET XX GET should return old value
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("xx_get_key")),
        RespFrame::BulkString(Bytes::from("new_value")),
        RespFrame::BulkString(Bytes::from_static(b"XX")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("old_value")));

    // Verify new value was set
    let result = ctx.get("xx_get_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("new_value")));
}

#[tokio::test]
async fn test_set_nx_with_get_existing_key() {
    let ctx = TestContext::new().await;

    // Set initial value
    ctx.set("nx_get_key", "old_value").await.unwrap();

    // SET NX GET on existing key should return old value and not set
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("nx_get_key")),
        RespFrame::BulkString(Bytes::from("new_value")),
        RespFrame::BulkString(Bytes::from_static(b"NX")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("old_value")));

    // Verify value unchanged
    let result = ctx.get("nx_get_key").await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("old_value")));
}

#[tokio::test]
async fn test_set_xx_with_get_nonexistent() {
    let ctx = TestContext::new().await;

    // SET XX GET on non-existent key should return Null and not set
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("xx_get_key")),
        RespFrame::BulkString(Bytes::from("value")),
        RespFrame::BulkString(Bytes::from_static(b"XX")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Null);

    // Verify key was not created
    let result = ctx.get("xx_get_key").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

// ===== Additional GETEX Tests =====

#[tokio::test]
async fn test_getex_with_exat() {
    let ctx = TestContext::new().await;

    ctx.set("getex_key", "value").await.unwrap();

    let future_timestamp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs())
        + 60;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("getex_key")),
        RespFrame::BulkString(Bytes::from_static(b"EXAT")),
        RespFrame::BulkString(Bytes::from(future_timestamp.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_getex_with_pxat() {
    let ctx = TestContext::new().await;

    ctx.set("getex_key", "value").await.unwrap();

    let future_timestamp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis())
        + 60000;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("getex_key")),
        RespFrame::BulkString(Bytes::from_static(b"PXAT")),
        RespFrame::BulkString(Bytes::from(future_timestamp.to_string())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}

#[tokio::test]
async fn test_getex_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "item1").await.unwrap();

    // GETEX on non-string type should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETEX")),
        RespFrame::BulkString(Bytes::from("list_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

// ===== Additional Error Path Tests =====

#[tokio::test]
async fn test_strlen_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "item1").await.unwrap();

    // STRLEN on non-string type should fail
    let result = ctx.strlen("list_key").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_getrange_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "item1").await.unwrap();

    // GETRANGE on non-string type should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETRANGE")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("5")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_setrange_on_non_string_type() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "item1").await.unwrap();

    // SETRANGE on non-string type should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETRANGE")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("value")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_incr_on_float_string() {
    let ctx = TestContext::new().await;

    // Set a float value
    ctx.set("float_key", "10.5").await.unwrap();

    // INCR on float should fail (needs to be integer)
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INCR")),
        RespFrame::BulkString(Bytes::from("float_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_decr_on_float_string() {
    let ctx = TestContext::new().await;

    ctx.set("float_key", "10.5").await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"DECR")),
        RespFrame::BulkString(Bytes::from("float_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

// ===== Bit Operations Tests (GETBIT, SETBIT) =====

#[tokio::test]
async fn test_getbit_nonexistent_key() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
        RespFrame::BulkString(Bytes::from("nonexistent")),
        RespFrame::BulkString(Bytes::from("0")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_getbit_out_of_bounds() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", constants::CHAR_A).await.unwrap(); // "A" = 0x41 = 01000001

    // Get bit beyond string length
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("100")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_getbit_basic() {
    let ctx = TestContext::new().await;

    // "A" = 0x41 = 01000001 (MSB to LSB)
    ctx.set("bit_key", constants::CHAR_A).await.unwrap();

    // Bit 0 (MSB) should be 0
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("0")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Bit 1 should be 1
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_setbit_create_new_key() {
    let ctx = TestContext::new().await;

    // SETBIT on non-existent key should create it
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETBIT")),
        RespFrame::BulkString(Bytes::from("new_bit_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0)); // Previous bit was 0

    // Verify the bit was set
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
        RespFrame::BulkString(Bytes::from("new_bit_key")),
        RespFrame::BulkString(Bytes::from("0")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_setbit_modify_existing() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", constants::CHAR_A).await.unwrap(); // 0x41 = 01000001

    // Set bit 0 to 1 (should change from 0 to 1)
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETBIT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0)); // Previous value was 0

    // Verify it was changed
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("0")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_setbit_extend_string() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", constants::CHAR_A).await.unwrap();

    // Set a bit beyond current length - should extend string
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETBIT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("20")), // Beyond first byte
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    ctx.execute(command).await.unwrap();

    // Verify string was extended
    let result = ctx.get("bit_key").await.unwrap();
    match result {
        RespValue::BulkString(bytes) => {
            assert!(bytes.len() >= 3); // At least 3 bytes now
        }
        _ => panic!("Expected bulk string"),
    }
}

#[tokio::test]
async fn test_setbit_invalid_value() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", constants::CHAR_A).await.unwrap();

    // SETBIT with value > 1 should fail at parse time
    let command_result = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETBIT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("2")),
    ]));

    assert!(command_result.is_err());
}

#[tokio::test]
async fn test_getbit_type_error() {
    let ctx = TestContext::new().await;

    // Create a list (not a string)
    ctx.create_list("list_key", "value").await.unwrap();

    // GETBIT on list should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("0")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_setbit_type_error() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "value").await.unwrap();

    // SETBIT on list should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SETBIT")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

// ===== BITCOUNT Tests =====

#[tokio::test]
async fn test_bitcount_nonexistent_key() {
    let ctx = TestContext::new().await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITCOUNT")),
        RespFrame::BulkString(Bytes::from("nonexistent")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_bitcount_basic() {
    let ctx = TestContext::new().await;

    // "A" = 0x41 = 01000001 (2 bits set)
    ctx.set("bit_key", constants::CHAR_A).await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITCOUNT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(
        result,
        RespValue::Integer(constants::CHAR_A_BIT_COUNT),
        "Character 'A' (0x41) should have 2 bits set"
    );
}

#[tokio::test]
async fn test_bitcount_with_range() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", "ABC").await.unwrap();

    // Count bits in first byte only ('A' = 0x41 = 01000001 = 2 bits set)
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITCOUNT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("0")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::Integer(n) => {
            assert_eq!(n, 2, "First byte 'A' (0x41) should have 2 bits set");
        }
        _ => panic!("Expected integer response"),
    }
}

#[tokio::test]
async fn test_bitcount_with_negative_range() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", "ABC").await.unwrap();

    // Count bits using negative indices
    // Range -2 to -1 means bytes 1-2 (B and C)
    // 'B' = 0x42 = 01000010 = 2 bits set
    // 'C' = 0x43 = 01000011 = 3 bits set
    // Total = 2 + 3 = 5 bits
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITCOUNT")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("-2")),
        RespFrame::BulkString(Bytes::from("-1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::Integer(n) => {
            assert_eq!(
                n, 5,
                "Bytes 'B' (0x42, 2 bits) and 'C' (0x43, 3 bits) should total 5 bits"
            );
        }
        _ => panic!("Expected integer response"),
    }
}

#[tokio::test]
async fn test_bitcount_type_error() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "value").await.unwrap();

    // BITCOUNT on list should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITCOUNT")),
        RespFrame::BulkString(Bytes::from("list_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

// ===== BITPOS Tests =====

#[tokio::test]
async fn test_bitpos_nonexistent_key() {
    let ctx = TestContext::new().await;

    // BITPOS on non-existent key returns -1
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITPOS")),
        RespFrame::BulkString(Bytes::from("nonexistent")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(-1));
}

#[tokio::test]
async fn test_bitpos_basic() {
    let ctx = TestContext::new().await;

    // "A" = 0x41 = 01000001 (bit 1 is the first bit set to 1)
    ctx.set("bit_key", constants::CHAR_A).await.unwrap();

    // Find first bit set to 1
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITPOS")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::Integer(n) => {
            // For "A" (0x41 = 01000001), Redis uses LSB-first indexing
            // So bit positions are: 0=1, 1=0, 2=0, 3=0, 4=0, 5=0, 6=0, 7=1
            // First bit set to 1 is at position 0 (LSB)
            // But Redis BITPOS returns position from left (MSB), so it's 7
            assert_eq!(
                n, 7,
                "BITPOS should return 7 for 'A' (LSB-first, position from left)"
            );
        }
        _ => panic!("Expected integer response"),
    }
}

#[tokio::test]
async fn test_bitpos_with_range() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", "ABC").await.unwrap();

    // Find bit in specific range
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITPOS")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("1")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    // BITPOS with range should return a valid position or -1 if not found
    // For "ABC", first bit set to 1 in range [0,1] should be found
    match result {
        RespValue::Integer(n) => {
            assert!(
                n >= 0 && n < 16,
                "BITPOS should return a valid position (0-15) or -1, got {}",
                n
            );
        }
        _ => panic!("Expected integer response"),
    }
}

#[tokio::test]
async fn test_bitpos_invalid_bit() {
    let ctx = TestContext::new().await;

    ctx.set("bit_key", constants::CHAR_A).await.unwrap();

    // BITPOS with bit value > 1 should fail at parse time
    let command_result = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITPOS")),
        RespFrame::BulkString(Bytes::from("bit_key")),
        RespFrame::BulkString(Bytes::from("2")),
    ]));

    assert!(command_result.is_err());
}

#[tokio::test]
async fn test_bitpos_type_error() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "value").await.unwrap();

    // BITPOS on list should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITPOS")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

// ===== BITOP Tests =====

#[tokio::test]
async fn test_bitop_and_basic() {
    let ctx = TestContext::new().await;

    // Set up source keys
    // Set binary data using Bytes directly
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from(vec![0xFF, 0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key2")),
        RespFrame::BulkString(Bytes::from(vec![0x00, 0xFF])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // BITOP AND
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"AND")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("key2")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    // BITOP AND: 0xFF & 0x00 = 0x00, 0x00 & 0xFF = 0x00, result length = 2
    match result {
        RespValue::Integer(n) => assert_eq!(n, 2, "BITOP AND result should be 2 bytes"),
        _ => panic!("Expected integer response"),
    }

    // Verify result: AND of [0xFF, 0x00] and [0x00, 0xFF] = [0x00, 0x00]
    let result = ctx.get("dest").await.unwrap();
    match result {
        RespValue::BulkString(bytes) => {
            assert_eq!(bytes.len(), 2);
            assert_eq!(bytes[0], 0x00);
            assert_eq!(bytes[1], 0x00);
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_bitop_or_basic() {
    let ctx = TestContext::new().await;

    // Set binary data using Bytes directly
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from(vec![0x00, 0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key2")),
        RespFrame::BulkString(Bytes::from(vec![0xFF, 0xFF])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"OR")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("key2")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    // BITOP OR: 0x00 | 0xFF = 0xFF, 0x00 | 0xFF = 0xFF, result length = 2
    match result {
        RespValue::Integer(n) => assert_eq!(n, 2, "BITOP OR result should be 2 bytes"),
        _ => panic!("Expected integer response"),
    }

    // Verify result: OR of [0x00, 0x00] and [0xFF, 0xFF] = [0xFF, 0xFF]
    let result = ctx.get("dest").await.unwrap();
    match result {
        RespValue::BulkString(bytes) => {
            assert_eq!(bytes.len(), 2);
            assert_eq!(bytes[0], 0xFF);
            assert_eq!(bytes[1], 0xFF);
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_bitop_xor_basic() {
    let ctx = TestContext::new().await;

    // Set binary data using Bytes directly
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from(vec![0xFF])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key2")),
        RespFrame::BulkString(Bytes::from(vec![0xFF])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"XOR")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("key2")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    // BITOP XOR: 0xFF ^ 0xFF = 0x00, result length = 1
    match result {
        RespValue::Integer(n) => assert_eq!(n, 1, "BITOP XOR result should be 1 byte"),
        _ => panic!("Expected integer response"),
    }

    // Verify result: XOR of [0xFF] and [0xFF] = [0x00]
    let result = ctx.get("dest").await.unwrap();
    match result {
        RespValue::BulkString(bytes) => {
            assert_eq!(bytes.len(), 1);
            assert_eq!(bytes[0], 0x00);
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_bitop_not_basic() {
    let ctx = TestContext::new().await;

    // Set binary data
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from(vec![0xFF, 0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // BITOP NOT requires exactly one source key
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"NOT")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("key1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    // BITOP NOT: NOT [0xFF, 0x00] = [0x00, 0xFF], result length = 2
    match result {
        RespValue::Integer(n) => assert_eq!(n, 2, "BITOP NOT result should be 2 bytes"),
        _ => panic!("Expected integer response"),
    }

    // Verify result: NOT of [0xFF, 0x00] = [0x00, 0xFF]
    let result = ctx.get("dest").await.unwrap();
    match result {
        RespValue::BulkString(bytes) => {
            assert_eq!(bytes.len(), 2);
            assert_eq!(bytes[0], 0x00);
            assert_eq!(bytes[1], 0xFF);
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_bitop_not_wrong_arg_count() {
    let ctx = TestContext::new().await;

    // Set binary data
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from(vec![0xFF])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key2")),
        RespFrame::BulkString(Bytes::from(vec![0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // BITOP NOT with multiple keys should fail at parse time
    let command_result = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"NOT")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("key2")),
    ]));

    assert!(command_result.is_err());
}

#[tokio::test]
async fn test_bitop_invalid_operation() {
    let ctx = TestContext::new().await;

    // Set binary data
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from(vec![0xFF])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // Invalid operation should fail at parse time
    let command_result = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"INVALID")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("key1")),
    ]));

    assert!(command_result.is_err());
}

#[tokio::test]
async fn test_bitop_type_error() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "value").await.unwrap();

    ctx.set("string_key", "value").await.unwrap();

    // BITOP with non-string should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"AND")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("list_key")),
        RespFrame::BulkString(Bytes::from("string_key")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_bitop_nonexistent_keys() {
    let ctx = TestContext::new().await;

    // BITOP with non-existent keys should treat them as empty strings
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITOP")),
        RespFrame::BulkString(Bytes::from_static(b"AND")),
        RespFrame::BulkString(Bytes::from("dest")),
        RespFrame::BulkString(Bytes::from("nonexistent1")),
        RespFrame::BulkString(Bytes::from("nonexistent2")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

// ===== BITFIELD Tests =====

#[tokio::test]
async fn test_bitfield_get_basic() {
    let ctx = TestContext::new().await;

    // Set binary data
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from(vec![0xFF, 0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // BITFIELD GET u8 0
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITFIELD")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
        RespFrame::BulkString(Bytes::from_static(b"u8")),
        RespFrame::BulkString(Bytes::from("0")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 1);
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_bitfield_set_basic() {
    let ctx = TestContext::new().await;

    // Set binary data
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from(vec![0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // BITFIELD SET u8 0 255
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITFIELD")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from_static(b"u8")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("255")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 1);
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_bitfield_incrby_basic() {
    let ctx = TestContext::new().await;

    // Set binary data
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from(vec![0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // BITFIELD INCRBY u8 0 1
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITFIELD")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from_static(b"INCRBY")),
        RespFrame::BulkString(Bytes::from_static(b"u8")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("1")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::Array(_) => {}
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_bitfield_nonexistent_key() {
    let ctx = TestContext::new().await;

    // BITFIELD on non-existent key should create it
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITFIELD")),
        RespFrame::BulkString(Bytes::from("new_key")),
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from_static(b"u8")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("100")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();
    match result {
        RespValue::Array(_) => {}
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_bitfield_invalid_operation() {
    let ctx = TestContext::new().await;

    // Set binary data
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"SET")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from(vec![0x00])),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // Invalid operation should fail at parse time
    let command_result = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"BITFIELD")),
        RespFrame::BulkString(Bytes::from("bitfield_key")),
        RespFrame::BulkString(Bytes::from_static(b"INVALID")),
    ]));

    assert!(command_result.is_err());
}

// ===== Additional Error Path Tests =====

#[tokio::test]
async fn test_get_type_error_on_list() {
    let ctx = TestContext::new().await;

    // Create a list
    ctx.create_list("list_key", "value").await.unwrap();

    // GET on list should fail
    let result = ctx.get("list_key").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_append_type_error() {
    let ctx = TestContext::new().await;

    // Create a hash
    ctx.create_hash("hash_key", "field", "value").await.unwrap();

    // APPEND on hash should fail
    let result = ctx.append("hash_key", "more").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_strlen_type_error() {
    let ctx = TestContext::new().await;

    // Create a set
    ctx.create_set("set_key", "member").await.unwrap();

    // STRLEN on set should fail
    let result = ctx.strlen("set_key").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_getrange_type_error() {
    let ctx = TestContext::new().await;

    // Create a sorted set
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"ZADD")),
        RespFrame::BulkString(Bytes::from("zset_key")),
        RespFrame::BulkString(Bytes::from("1")),
        RespFrame::BulkString(Bytes::from("member")),
    ]))
    .unwrap();
    ctx.execute(command).await.unwrap();

    // GETRANGE on sorted set should fail
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"GETRANGE")),
        RespFrame::BulkString(Bytes::from("zset_key")),
        RespFrame::BulkString(Bytes::from("0")),
        RespFrame::BulkString(Bytes::from("5")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await;
    assert!(result.is_err());
}
