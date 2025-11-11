// tests/integration/zset_commands_test.rs

//! Integration tests for sorted set (zset) commands
//! Tests: ZADD, ZCARD, ZSCORE, ZMSCORE, ZRANK, ZREVRANK, ZCOUNT, ZRANGE, ZREVRANGE,
//!        ZREM, ZINCRBY, ZPOPMAX, ZPOPMIN, ZRANGEBYSCORE, ZREMRANGEBYRANK,
//!        ZREMRANGEBYSCORE, ZUNIONSTORE, ZINTERSTORE, ZLEXCOUNT, ZRANGEBYLEX,
//!        ZREMRANGEBYLEX, ZRANGESTORE

use super::test_helpers::TestContext;
use spineldb::core::{RespValue, SpinelDBError};

// ===== Helper Functions =====

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
            for (i, (val, exp)) in values.iter().zip(expected.iter()).enumerate() {
                match val {
                    RespValue::BulkString(bs) => {
                        let s = String::from_utf8_lossy(bs);
                        assert_eq!(
                            s, *exp,
                            "{}: mismatch at index {}, expected '{}', got '{}'",
                            message, i, exp, s
                        );
                    }
                    _ => panic!(
                        "{}: Expected BulkString at index {}, got {:?}",
                        message, i, val
                    ),
                }
            }
        }
        _ => panic!("{}: Expected array response, got {:?}", message, result),
    }
}

/// Helper to assert that a RespValue is an array with scores (alternating member, score)
fn assert_array_with_scores_equals(
    result: &RespValue,
    expected: &[(&'static str, &'static str)],
    message: &str,
) {
    match result {
        RespValue::Array(values) => {
            assert_eq!(
                values.len(),
                expected.len() * 2,
                "{}: length mismatch, expected {} (members + scores), got {}",
                message,
                expected.len() * 2,
                values.len()
            );
            for (i, (member, score)) in expected.iter().enumerate() {
                let member_idx = i * 2;
                let score_idx = i * 2 + 1;
                match (&values[member_idx], &values[score_idx]) {
                    (RespValue::BulkString(bs_member), RespValue::BulkString(bs_score)) => {
                        let s_member = String::from_utf8_lossy(bs_member);
                        let s_score = String::from_utf8_lossy(bs_score);
                        assert_eq!(
                            s_member, *member,
                            "{}: member mismatch at index {}, expected '{}', got '{}'",
                            message, i, member, s_member
                        );
                        assert_eq!(
                            s_score, *score,
                            "{}: score mismatch at index {}, expected '{}', got '{}'",
                            message, i, score, s_score
                        );
                    }
                    _ => panic!(
                        "{}: Expected BulkString at index {}, got {:?}",
                        message, member_idx, values[member_idx]
                    ),
                }
            }
        }
        _ => panic!("{}: Expected array response, got {:?}", message, result),
    }
}

// ===== ZADD Tests =====

#[tokio::test]
async fn test_zadd_basic() {
    let ctx = TestContext::new().await;

    // ZADD a single member
    let result = ctx
        .zadd("myzset", &[("1.0", "member1")], &[])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify with ZCARD
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_zadd_multiple_members() {
    let ctx = TestContext::new().await;

    // ZADD multiple members
    let result = ctx
        .zadd(
            "myzset",
            &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify with ZCARD
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_zadd_update_existing() {
    let ctx = TestContext::new().await;

    // Add initial member
    ctx.zadd("myzset", &[("1.0", "member1")], &[])
        .await
        .unwrap();

    // Update existing member with new score
    let result = ctx
        .zadd("myzset", &[("2.0", "member1")], &[])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0)); // 0 new members

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "2");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_nx_option() {
    let ctx = TestContext::new().await;

    // Add initial member
    ctx.zadd("myzset", &[("1.0", "member1")], &[])
        .await
        .unwrap();

    // Try to add with NX (should not update)
    let result = ctx
        .zadd("myzset", &[("2.0", "member1")], &["NX"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify score unchanged
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "1");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_xx_option() {
    let ctx = TestContext::new().await;

    // Try to add with XX (should not add new member)
    let result = ctx
        .zadd("myzset", &[("1.0", "member1")], &["XX"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify member not added
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Add member first
    ctx.zadd("myzset", &[("1.0", "member1")], &[])
        .await
        .unwrap();

    // Update with XX (should work)
    let result = ctx
        .zadd("myzset", &[("2.0", "member1")], &["XX"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0)); // 0 new, but updated

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "2");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_ch_option() {
    let ctx = TestContext::new().await;

    // Add initial member
    ctx.zadd("myzset", &[("1.0", "member1")], &[])
        .await
        .unwrap();

    // Update with CH (should return changed count)
    let result = ctx
        .zadd("myzset", &[("2.0", "member1")], &["CH"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1)); // 1 changed

    // Add new member with CH
    let result = ctx
        .zadd("myzset", &[("3.0", "member2")], &["CH"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1)); // 1 changed (new member)
}

#[tokio::test]
async fn test_zadd_incr_option() {
    let ctx = TestContext::new().await;

    // Add member with INCR
    let result = ctx
        .zadd("myzset", &[("1.0", "member1")], &["INCR"])
        .await
        .unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "1");
        }
        _ => panic!("Expected BulkString for INCR result"),
    }

    // Increment again
    let result = ctx
        .zadd("myzset", &[("2.0", "member1")], &["INCR"])
        .await
        .unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "3"); // 1 + 2 = 3
        }
        _ => panic!("Expected BulkString for INCR result"),
    }
}

#[tokio::test]
async fn test_zadd_gt_option() {
    let ctx = TestContext::new().await;

    // Add initial member
    ctx.zadd("myzset", &[("5.0", "member1")], &[])
        .await
        .unwrap();

    // Try to update with GT and higher score (should update)
    let result = ctx
        .zadd("myzset", &[("10.0", "member1")], &["GT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "10");
        }
        _ => panic!("Expected BulkString for score"),
    }

    // Try to update with GT and lower score (should not update)
    let result = ctx
        .zadd("myzset", &[("3.0", "member1")], &["GT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify score unchanged
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "10");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_lt_option() {
    let ctx = TestContext::new().await;

    // Add initial member
    ctx.zadd("myzset", &[("10.0", "member1")], &[])
        .await
        .unwrap();

    // Try to update with LT and lower score (should update)
    let result = ctx
        .zadd("myzset", &[("5.0", "member1")], &["LT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "5");
        }
        _ => panic!("Expected BulkString for score"),
    }

    // Try to update with LT and higher score (should not update)
    let result = ctx
        .zadd("myzset", &[("8.0", "member1")], &["LT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify score unchanged
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "5");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZADD to a string key (should fail)
    let result = ctx.zadd("mykey", &[("1.0", "member1")], &[]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZCARD Tests =====

#[tokio::test]
async fn test_zcard_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get cardinality
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_zcard_empty_set() {
    let ctx = TestContext::new().await;

    // Get cardinality of non-existent set
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zcard_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZCARD on a string key (should fail)
    let result = ctx.zcard("mykey").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZSCORE Tests =====

#[tokio::test]
async fn test_zscore_basic() {
    let ctx = TestContext::new().await;

    // Add member
    ctx.zadd("myzset", &[("1.5", "member1")], &[])
        .await
        .unwrap();

    // Get score
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "1.5");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zscore_nonexistent_member() {
    let ctx = TestContext::new().await;

    // Get score of non-existent member
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_zscore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZSCORE on a string key (should fail)
    let result = ctx.zscore("mykey", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZMSCORE Tests =====

#[tokio::test]
async fn test_zmscore_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get multiple scores
    let result = ctx
        .zmscore("myzset", &["member1", "member2", "member4"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 3);
            match &values[0] {
                RespValue::BulkString(bs) => {
                    assert_eq!(String::from_utf8_lossy(bs), "1");
                }
                _ => panic!("Expected BulkString"),
            }
            match &values[1] {
                RespValue::BulkString(bs) => {
                    assert_eq!(String::from_utf8_lossy(bs), "2");
                }
                _ => panic!("Expected BulkString"),
            }
            assert_eq!(values[2], RespValue::Null); // member4 doesn't exist
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_zmscore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZMSCORE on a string key (should fail)
    let result = ctx.zmscore("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZRANK Tests =====

#[tokio::test]
async fn test_zrank_basic() {
    let ctx = TestContext::new().await;

    // Add members with different scores
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get rank (0-based)
    let result = ctx.zrank("myzset", "member1").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    let result = ctx.zrank("myzset", "member2").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    let result = ctx.zrank("myzset", "member3").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zrank_nonexistent_member() {
    let ctx = TestContext::new().await;

    // Get rank of non-existent member
    let result = ctx.zrank("myzset", "member1").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_zrank_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZRANK on a string key (should fail)
    let result = ctx.zrank("mykey", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZREVRANK Tests =====

#[tokio::test]
async fn test_zrevrank_basic() {
    let ctx = TestContext::new().await;

    // Add members with different scores
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get reverse rank (0-based, highest score first)
    let result = ctx.zrevrank("myzset", "member3").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    let result = ctx.zrevrank("myzset", "member2").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    let result = ctx.zrevrank("myzset", "member1").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zrevrank_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZREVRANK on a string key (should fail)
    let result = ctx.zrevrank("mykey", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZCOUNT Tests =====

#[tokio::test]
async fn test_zcount_basic() {
    let ctx = TestContext::new().await;

    // Add members with different scores
    ctx.zadd(
        "myzset",
        &[
            ("1.0", "member1"),
            ("2.0", "member2"),
            ("3.0", "member3"),
            ("4.0", "member4"),
            ("5.0", "member5"),
        ],
        &[],
    )
    .await
    .unwrap();

    // Count members with score between 2 and 4 (inclusive)
    let result = ctx.zcount("myzset", "2", "4").await.unwrap();
    assert_eq!(result, RespValue::Integer(3)); // member2, member3, member4
}

#[tokio::test]
async fn test_zcount_exclusive() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Count with exclusive boundaries
    let result = ctx.zcount("myzset", "(2", "(3").await.unwrap();
    assert_eq!(result, RespValue::Integer(0)); // No members between 2 and 3 (exclusive)
}

#[tokio::test]
async fn test_zcount_infinity() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Count all members (negative infinity to positive infinity)
    let result = ctx.zcount("myzset", "-inf", "+inf").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_zcount_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZCOUNT on a string key (should fail)
    let result = ctx.zcount("mykey", "1", "10").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZRANGE Tests =====

#[tokio::test]
async fn test_zrange_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get range
    let result = ctx.zrange("myzset", 0, -1, false).await.unwrap();
    assert_array_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_zrange_basic",
    );
}

#[tokio::test]
async fn test_zrange_with_scores() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get range with scores
    let result = ctx.zrange("myzset", 0, -1, true).await.unwrap();
    assert_array_with_scores_equals(
        &result,
        &[("member1", "1"), ("member2", "2"), ("member3", "3")],
        "test_zrange_with_scores",
    );
}

#[tokio::test]
async fn test_zrange_partial() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get partial range
    let result = ctx.zrange("myzset", 0, 1, false).await.unwrap();
    assert_array_equals(&result, &["member1", "member2"], "test_zrange_partial");
}

#[tokio::test]
async fn test_zrange_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZRANGE on a string key (should fail)
    let result = ctx.zrange("mykey", 0, -1, false).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZREVRANGE Tests =====

#[tokio::test]
async fn test_zrevrange_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get reverse range
    let result = ctx.zrevrange("myzset", 0, -1, false).await.unwrap();
    assert_array_equals(
        &result,
        &["member3", "member2", "member1"],
        "test_zrevrange_basic",
    );
}

#[tokio::test]
async fn test_zrevrange_with_scores() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get reverse range with scores
    let result = ctx.zrevrange("myzset", 0, -1, true).await.unwrap();
    assert_array_with_scores_equals(
        &result,
        &[("member3", "3"), ("member2", "2"), ("member1", "1")],
        "test_zrevrange_with_scores",
    );
}

#[tokio::test]
async fn test_zrevrange_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZREVRANGE on a string key (should fail)
    let result = ctx.zrevrange("mykey", 0, -1, false).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZREM Tests =====

#[tokio::test]
async fn test_zrem_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Remove one member
    let result = ctx.zrem("myzset", &["member1"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zrem_multiple_members() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[
            ("1.0", "member1"),
            ("2.0", "member2"),
            ("3.0", "member3"),
            ("4.0", "member4"),
        ],
        &[],
    )
    .await
    .unwrap();

    // Remove multiple members
    let result = ctx.zrem("myzset", &["member1", "member3"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(2));

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zrem_nonexistent_members() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd("myzset", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Remove non-existent members
    let result = ctx.zrem("myzset", &["member3", "member4"]).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify set unchanged
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zrem_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZREM on a string key (should fail)
    let result = ctx.zrem("mykey", &["member1"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZINCRBY Tests =====

#[tokio::test]
async fn test_zincrby_basic() {
    let ctx = TestContext::new().await;

    // Add member
    ctx.zadd("myzset", &[("1.0", "member1")], &[])
        .await
        .unwrap();

    // Increment score
    let result = ctx.zincrby("myzset", "2.5", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "3.5"); // 1.0 + 2.5 = 3.5
        }
        _ => panic!("Expected BulkString for score"),
    }

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "3.5");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zincrby_new_member() {
    let ctx = TestContext::new().await;

    // Increment non-existent member (should create it)
    let result = ctx.zincrby("myzset", "5.0", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "5");
        }
        _ => panic!("Expected BulkString for score"),
    }

    // Verify member created
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_zincrby_negative() {
    let ctx = TestContext::new().await;

    // Add member
    ctx.zadd("myzset", &[("10.0", "member1")], &[])
        .await
        .unwrap();

    // Decrement score (negative increment)
    let result = ctx.zincrby("myzset", "-3.0", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "7"); // 10.0 - 3.0 = 7.0
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zincrby_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZINCRBY on a string key (should fail)
    let result = ctx.zincrby("mykey", "1.0", "member1").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZPOPMAX Tests =====

#[tokio::test]
async fn test_zpopmax_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Pop max (should return member3 with highest score)
    let result = ctx.zpopmax("myzset", None).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2); // member and score
            match &values[0] {
                RespValue::BulkString(bs) => {
                    assert_eq!(String::from_utf8_lossy(bs), "member3");
                }
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected array response"),
    }

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zpopmax_count() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Pop 2 max members
    let result = ctx.zpopmax("myzset", Some(2)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 4); // 2 members * 2 (member + score)
        }
        _ => panic!("Expected array response"),
    }

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_zpopmax_empty_set() {
    let ctx = TestContext::new().await;

    // Pop from empty set (without count, returns Null)
    let result = ctx.zpopmax("myzset", None).await.unwrap();
    assert_eq!(result, RespValue::Null);

    // Pop from empty set (with count, returns empty array)
    let result = ctx.zpopmax("myzset", Some(1)).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_zpopmax_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZPOPMAX on a string key (should fail)
    let result = ctx.zpopmax("mykey", None).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZPOPMIN Tests =====

#[tokio::test]
async fn test_zpopmin_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Pop min (should return member1 with lowest score)
    let result = ctx.zpopmin("myzset", None).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2); // member and score
            match &values[0] {
                RespValue::BulkString(bs) => {
                    assert_eq!(String::from_utf8_lossy(bs), "member1");
                }
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected array response"),
    }

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zpopmin_count() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Pop 2 min members
    let result = ctx.zpopmin("myzset", Some(2)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 4); // 2 members * 2 (member + score)
        }
        _ => panic!("Expected array response"),
    }

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_zpopmin_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZPOPMIN on a string key (should fail)
    let result = ctx.zpopmin("mykey", None).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZRANGEBYSCORE Tests =====

#[tokio::test]
async fn test_zrangebyscore_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[
            ("1.0", "member1"),
            ("2.0", "member2"),
            ("3.0", "member3"),
            ("4.0", "member4"),
            ("5.0", "member5"),
        ],
        &[],
    )
    .await
    .unwrap();

    // Get range by score
    let result = ctx
        .zrangebyscore("myzset", "2", "4", false, None)
        .await
        .unwrap();
    assert_array_equals(
        &result,
        &["member2", "member3", "member4"],
        "test_zrangebyscore_basic",
    );
}

#[tokio::test]
async fn test_zrangebyscore_with_scores() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get range by score with scores
    let result = ctx
        .zrangebyscore("myzset", "1", "3", true, None)
        .await
        .unwrap();
    assert_array_with_scores_equals(
        &result,
        &[("member1", "1"), ("member2", "2"), ("member3", "3")],
        "test_zrangebyscore_with_scores",
    );
}

#[tokio::test]
async fn test_zrangebyscore_with_limit() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[
            ("1.0", "member1"),
            ("2.0", "member2"),
            ("3.0", "member3"),
            ("4.0", "member4"),
        ],
        &[],
    )
    .await
    .unwrap();

    // Get range by score with limit
    let result = ctx
        .zrangebyscore("myzset", "1", "4", false, Some((1, 2)))
        .await
        .unwrap();
    assert_array_equals(
        &result,
        &["member2", "member3"],
        "test_zrangebyscore_with_limit",
    );
}

#[tokio::test]
async fn test_zrangebyscore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZRANGEBYSCORE on a string key (should fail)
    let result = ctx.zrangebyscore("mykey", "1", "10", false, None).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZREMRANGEBYRANK Tests =====

#[tokio::test]
async fn test_zremrangebyrank_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[
            ("1.0", "member1"),
            ("2.0", "member2"),
            ("3.0", "member3"),
            ("4.0", "member4"),
        ],
        &[],
    )
    .await
    .unwrap();

    // Remove range by rank
    let result = ctx.zremrangebyrank("myzset", 1, 2).await.unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Removed member2 and member3

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zremrangebyrank_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZREMRANGEBYRANK on a string key (should fail)
    let result = ctx.zremrangebyrank("mykey", 0, 1).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZREMRANGEBYSCORE Tests =====

#[tokio::test]
async fn test_zremrangebyscore_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[
            ("1.0", "member1"),
            ("2.0", "member2"),
            ("3.0", "member3"),
            ("4.0", "member4"),
        ],
        &[],
    )
    .await
    .unwrap();

    // Remove range by score
    let result = ctx.zremrangebyscore("myzset", "2", "3").await.unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Removed member2 and member3

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zremrangebyscore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZREMRANGEBYSCORE on a string key (should fail)
    let result = ctx.zremrangebyscore("mykey", "1", "10").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZUNIONSTORE Tests =====

#[tokio::test]
async fn test_zunionstore_basic() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd("zset1", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();
    ctx.zadd("zset2", &[("3.0", "member2"), ("4.0", "member3")], &[])
        .await
        .unwrap();

    // Store union
    let result = ctx
        .zunionstore("destination", &["zset1", "zset2"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3)); // member1, member2, member3

    // Verify destination
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_zunionstore_with_weights() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();
    ctx.zadd("zset2", &[("2.0", "member1")], &[]).await.unwrap();

    // Store union with weights
    let result = ctx
        .zunionstore("destination", &["zset1", "zset2"], Some(&["2", "3"]), None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify score (1.0 * 2 + 2.0 * 3 = 2 + 6 = 8)
    let result = ctx.zscore("destination", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "8");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zunionstore_with_aggregate() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();
    ctx.zadd("zset2", &[("2.0", "member1")], &[]).await.unwrap();

    // Store union with MIN aggregate
    let result = ctx
        .zunionstore("destination", &["zset1", "zset2"], None, Some("MIN"))
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify score (min of 1.0 and 2.0 = 1.0)
    let result = ctx.zscore("destination", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "1");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zunionstore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();

    // Try to ZUNIONSTORE with a string key (should fail)
    let result = ctx
        .zunionstore("destination", &["mykey", "zset1"], None, None)
        .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZINTERSTORE Tests =====

#[tokio::test]
async fn test_zinterstore_basic() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd(
        "zset1",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();
    ctx.zadd(
        "zset2",
        &[("4.0", "member2"), ("5.0", "member3"), ("6.0", "member4")],
        &[],
    )
    .await
    .unwrap();

    // Store intersection
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2)); // member2 and member3

    // Verify destination
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zinterstore_with_weights() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();
    ctx.zadd("zset2", &[("2.0", "member1")], &[]).await.unwrap();

    // Store intersection with weights
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2"], Some(&["2", "3"]), None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify score (1.0 * 2 + 2.0 * 3 = 2 + 6 = 8)
    let result = ctx.zscore("destination", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "8");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zinterstore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();

    // Try to ZINTERSTORE with a string key (should fail)
    let result = ctx
        .zinterstore("destination", &["mykey", "zset1"], None, None)
        .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZLEXCOUNT Tests =====

#[tokio::test]
async fn test_zlexcount_basic() {
    let ctx = TestContext::new().await;

    // Add members with same score (for lexicographic ordering)
    ctx.zadd(
        "myzset",
        &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d")],
        &[],
    )
    .await
    .unwrap();

    // Count lexicographic range
    let result = ctx.zlexcount("myzset", "[b", "[d").await.unwrap();
    assert_eq!(result, RespValue::Integer(3)); // b, c, d
}

#[tokio::test]
async fn test_zlexcount_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZLEXCOUNT on a string key (should fail)
    let result = ctx.zlexcount("mykey", "[a", "[z").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZRANGEBYLEX Tests =====

#[tokio::test]
async fn test_zrangebylex_basic() {
    let ctx = TestContext::new().await;

    // Add members with same score
    ctx.zadd(
        "myzset",
        &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d")],
        &[],
    )
    .await
    .unwrap();

    // Get range by lex
    let result = ctx.zrangebylex("myzset", "[b", "[d", None).await.unwrap();
    assert_array_equals(&result, &["b", "c", "d"], "test_zrangebylex_basic");
}

#[tokio::test]
async fn test_zrangebylex_with_limit() {
    let ctx = TestContext::new().await;

    // Add members with same score
    ctx.zadd(
        "myzset",
        &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d")],
        &[],
    )
    .await
    .unwrap();

    // Get range by lex with limit
    let result = ctx
        .zrangebylex("myzset", "[a", "[z", Some((1, 2)))
        .await
        .unwrap();
    assert_array_equals(&result, &["b", "c"], "test_zrangebylex_with_limit");
}

#[tokio::test]
async fn test_zrangebylex_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZRANGEBYLEX on a string key (should fail)
    let result = ctx.zrangebylex("mykey", "[a", "[z", None).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZREMRANGEBYLEX Tests =====

#[tokio::test]
async fn test_zremrangebylex_basic() {
    let ctx = TestContext::new().await;

    // Add members with same score
    ctx.zadd(
        "myzset",
        &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d")],
        &[],
    )
    .await
    .unwrap();

    // Remove range by lex
    let result = ctx.zremrangebylex("myzset", "[b", "[c").await.unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Removed b and c

    // Verify removed
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zremrangebylex_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZREMRANGEBYLEX on a string key (should fail)
    let result = ctx.zremrangebylex("mykey", "[a", "[z").await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== ZRANGESTORE Tests =====

#[tokio::test]
async fn test_zrangestore_basic() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "source",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Store range
    let result = ctx
        .zrangestore("destination", "source", 0, 1, false, false)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Stored 2 members

    // Verify destination
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zrangestore_rev() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "source",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Store reverse range
    let result = ctx
        .zrangestore("destination", "source", 0, 1, false, true)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Stored 2 members (reverse order)

    // Verify destination
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zrangestore_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("mykey", "value").await.unwrap();

    // Try to ZRANGESTORE with a string key (should fail)
    let result = ctx
        .zrangestore("destination", "mykey", 0, -1, false, false)
        .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SpinelDBError::WrongType));
}

// ===== Edge Cases and Additional Tests =====

#[tokio::test]
async fn test_zset_operations_with_large_sets() {
    let ctx = TestContext::new().await;

    // Create large sets
    let mut members1 = Vec::new();
    let mut members2 = Vec::new();
    for i in 0..100 {
        members1.push((i.to_string(), format!("member{}", i)));
        if i % 2 == 0 {
            members2.push((i.to_string(), format!("member{}", i)));
        }
    }

    let members1_refs: Vec<(&str, &str)> = members1
        .iter()
        .map(|(s, m)| (s.as_str(), m.as_str()))
        .collect();
    let members2_refs: Vec<(&str, &str)> = members2
        .iter()
        .map(|(s, m)| (s.as_str(), m.as_str()))
        .collect();

    ctx.zadd("zset1", &members1_refs, &[]).await.unwrap();
    ctx.zadd("zset2", &members2_refs, &[]).await.unwrap();

    // Test intersection
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2"], None, None)
        .await
        .unwrap();
    match result {
        RespValue::Integer(count) => {
            assert_eq!(count, 50); // Half of 100
        }
        _ => panic!("Expected Integer response"),
    }
}

#[tokio::test]
async fn test_zset_operations_with_unicode() {
    let ctx = TestContext::new().await;

    // Add unicode members
    ctx.zadd("myzset", &[("1.0", "成员1"), ("2.0", "🌍")], &[])
        .await
        .unwrap();

    // Verify
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zset_empty_string_member() {
    let ctx = TestContext::new().await;

    // Add empty string as member
    ctx.zadd("myzset", &[("1.0", ""), ("2.0", "member1")], &[])
        .await
        .unwrap();

    // Verify
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zadd_duplicate_scores() {
    let ctx = TestContext::new().await;

    // Add members with same score
    let result = ctx
        .zadd(
            "myzset",
            &[("1.0", "member1"), ("1.0", "member2"), ("1.0", "member3")],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify all added
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
}

#[tokio::test]
async fn test_zrange_negative_indices() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get range with negative indices
    let result = ctx.zrange("myzset", -2, -1, false).await.unwrap();
    assert_array_equals(
        &result,
        &["member2", "member3"],
        "test_zrange_negative_indices",
    );
}

#[tokio::test]
async fn test_zremrangebyrank_all() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Remove all
    let result = ctx.zremrangebyrank("myzset", 0, -1).await.unwrap();
    assert_eq!(result, RespValue::Integer(3));

    // Verify empty
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zunionstore_empty_result() {
    let ctx = TestContext::new().await;

    // Create empty sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();
    ctx.zrem("zset1", &["member1"]).await.unwrap();

    // Store union of empty sets
    let result = ctx
        .zunionstore("destination", &["zset1"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination doesn't exist
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zinterstore_empty_result() {
    let ctx = TestContext::new().await;

    // Create sets with no intersection
    ctx.zadd("zset1", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();
    ctx.zadd("zset2", &[("3.0", "member3"), ("4.0", "member4")], &[])
        .await
        .unwrap();

    // Store intersection (should be empty)
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination doesn't exist
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

// ===== Additional Coverage Tests =====

#[tokio::test]
async fn test_zadd_ch_with_nx() {
    let ctx = TestContext::new().await;

    // Add with CH and NX
    let result = ctx
        .zadd("myzset", &[("1.0", "member1")], &["CH", "NX"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Try to add again with CH and NX (should not update)
    let result = ctx
        .zadd("myzset", &[("2.0", "member1")], &["CH", "NX"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0)); // No change

    // Verify score unchanged
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "1");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_ch_with_xx() {
    let ctx = TestContext::new().await;

    // Add member first
    ctx.zadd("myzset", &[("1.0", "member1")], &[])
        .await
        .unwrap();

    // Update with CH and XX
    let result = ctx
        .zadd("myzset", &[("2.0", "member1")], &["CH", "XX"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1)); // 1 changed

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "2");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_gt_with_existing() {
    let ctx = TestContext::new().await;

    // Add member
    ctx.zadd("myzset", &[("5.0", "member1")], &[])
        .await
        .unwrap();

    // Try GT with lower score (should not update)
    let result = ctx
        .zadd("myzset", &[("3.0", "member1")], &["GT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Try GT with higher score (should update)
    let result = ctx
        .zadd("myzset", &[("7.0", "member1")], &["GT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0)); // 0 new, but updated

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "7");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_lt_with_existing() {
    let ctx = TestContext::new().await;

    // Add member
    ctx.zadd("myzset", &[("10.0", "member1")], &[])
        .await
        .unwrap();

    // Try LT with higher score (should not update)
    let result = ctx
        .zadd("myzset", &[("12.0", "member1")], &["LT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Try LT with lower score (should update)
    let result = ctx
        .zadd("myzset", &[("8.0", "member1")], &["LT"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0)); // 0 new, but updated

    // Verify score updated
    let result = ctx.zscore("myzset", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "8");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zadd_incr_new_member() {
    let ctx = TestContext::new().await;

    // INCR with new member
    let result = ctx
        .zadd("myzset", &[("5.0", "member1")], &["INCR"])
        .await
        .unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "5");
        }
        _ => panic!("Expected BulkString for INCR result"),
    }
}

#[tokio::test]
async fn test_zrange_empty_set() {
    let ctx = TestContext::new().await;

    // Get range from empty set
    let result = ctx.zrange("myzset", 0, -1, false).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_zrange_out_of_bounds() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd("myzset", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Get range out of bounds
    let result = ctx.zrange("myzset", 10, 20, false).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_zrevrange_empty_set() {
    let ctx = TestContext::new().await;

    // Get reverse range from empty set
    let result = ctx.zrevrange("myzset", 0, -1, false).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_zrangebyscore_exclusive_boundaries() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get range with exclusive boundaries
    let result = ctx
        .zrangebyscore("myzset", "(1", "(3", false, None)
        .await
        .unwrap();
    assert_array_equals(
        &result,
        &["member2"],
        "test_zrangebyscore_exclusive_boundaries",
    );
}

#[tokio::test]
async fn test_zrangebyscore_infinity() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Get range with infinity
    let result = ctx
        .zrangebyscore("myzset", "-inf", "+inf", false, None)
        .await
        .unwrap();
    assert_array_equals(
        &result,
        &["member1", "member2", "member3"],
        "test_zrangebyscore_infinity",
    );
}

#[tokio::test]
async fn test_zremrangebyrank_empty_set() {
    let ctx = TestContext::new().await;

    // Remove range from empty set
    let result = ctx.zremrangebyrank("myzset", 0, 1).await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zremrangebyscore_empty_set() {
    let ctx = TestContext::new().await;

    // Remove range by score from empty set
    let result = ctx.zremrangebyscore("myzset", "1", "10").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zremrangebyscore_exclusive_boundaries() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "myzset",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Remove with exclusive boundaries
    let result = ctx.zremrangebyscore("myzset", "(1", "(3").await.unwrap();
    assert_eq!(result, RespValue::Integer(1)); // Removed member2
}

#[tokio::test]
async fn test_zunionstore_with_max_aggregate() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();
    ctx.zadd("zset2", &[("2.0", "member1")], &[]).await.unwrap();

    // Store union with MAX aggregate
    let result = ctx
        .zunionstore("destination", &["zset1", "zset2"], None, Some("MAX"))
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify score (max of 1.0 and 2.0 = 2.0)
    let result = ctx.zscore("destination", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "2");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zinterstore_with_max_aggregate() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();
    ctx.zadd("zset2", &[("2.0", "member1")], &[]).await.unwrap();

    // Store intersection with MAX aggregate
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2"], None, Some("MAX"))
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify score (max of 1.0 and 2.0 = 2.0)
    let result = ctx.zscore("destination", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "2");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zinterstore_with_min_aggregate() {
    let ctx = TestContext::new().await;

    // Create sets
    ctx.zadd("zset1", &[("5.0", "member1")], &[]).await.unwrap();
    ctx.zadd("zset2", &[("3.0", "member1")], &[]).await.unwrap();

    // Store intersection with MIN aggregate
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2"], None, Some("MIN"))
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify score (min of 5.0 and 3.0 = 3.0)
    let result = ctx.zscore("destination", "member1").await.unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let score = String::from_utf8_lossy(&bs);
            assert_eq!(score, "3");
        }
        _ => panic!("Expected BulkString for score"),
    }
}

#[tokio::test]
async fn test_zunionstore_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.zadd("zset1", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();
    ctx.zadd("zset2", &[("3.0", "member2"), ("4.0", "member3")], &[])
        .await
        .unwrap();
    ctx.zadd("zset3", &[("5.0", "member3"), ("6.0", "member4")], &[])
        .await
        .unwrap();

    // Store union
    let result = ctx
        .zunionstore("destination", &["zset1", "zset2", "zset3"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(4)); // member1, member2, member3, member4
}

#[tokio::test]
async fn test_zinterstore_three_sets() {
    let ctx = TestContext::new().await;

    // Create three sets
    ctx.zadd("zset1", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();
    ctx.zadd("zset2", &[("3.0", "member2"), ("4.0", "member3")], &[])
        .await
        .unwrap();
    ctx.zadd("zset3", &[("5.0", "member2"), ("6.0", "member4")], &[])
        .await
        .unwrap();

    // Store intersection
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2", "zset3"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1)); // Only member2 in all three
}

#[tokio::test]
async fn test_zrangestore_with_scores() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd(
        "source",
        &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
        &[],
    )
    .await
    .unwrap();

    // Store range (ZRANGESTORE doesn't support WITHSCORES)
    let result = ctx
        .zrangestore("destination", "source", 0, 1, false, false)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Stored 2 members

    // Verify destination
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

// Note: test_zrangestore_rev already exists earlier in the file

#[tokio::test]
async fn test_zlexcount_exclusive_boundaries() {
    let ctx = TestContext::new().await;

    // Add members with same score
    ctx.zadd(
        "myzset",
        &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d")],
        &[],
    )
    .await
    .unwrap();

    // Count with exclusive boundaries
    let result = ctx.zlexcount("myzset", "(a", "(d").await.unwrap();
    assert_eq!(result, RespValue::Integer(2)); // b and c
}

#[tokio::test]
async fn test_zrangebylex_exclusive_boundaries() {
    let ctx = TestContext::new().await;

    // Add members with same score
    ctx.zadd(
        "myzset",
        &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d")],
        &[],
    )
    .await
    .unwrap();

    // Get range with exclusive boundaries
    let result = ctx.zrangebylex("myzset", "(a", "(d", None).await.unwrap();
    assert_array_equals(
        &result,
        &["b", "c"],
        "test_zrangebylex_exclusive_boundaries",
    );
}

#[tokio::test]
async fn test_zremrangebylex_exclusive_boundaries() {
    let ctx = TestContext::new().await;

    // Add members with same score
    ctx.zadd(
        "myzset",
        &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d")],
        &[],
    )
    .await
    .unwrap();

    // Remove with exclusive boundaries
    let result = ctx.zremrangebylex("myzset", "(a", "(d").await.unwrap();
    assert_eq!(result, RespValue::Integer(2)); // Removed b and c

    // Verify
    let result = ctx.zcard("myzset").await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
}

#[tokio::test]
async fn test_zpopmax_count_zero() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd("myzset", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Pop with count 0
    let result = ctx.zpopmax("myzset", Some(0)).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_zpopmin_count_zero() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd("myzset", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Pop with count 0
    let result = ctx.zpopmin("myzset", Some(0)).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_zpopmax_count_larger_than_set() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd("myzset", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Pop with count larger than set size
    let result = ctx.zpopmax("myzset", Some(10)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 4); // 2 members * 2 (member + score)
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_zpopmin_count_larger_than_set() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd("myzset", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Pop with count larger than set size
    let result = ctx.zpopmin("myzset", Some(10)).await.unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 4); // 2 members * 2 (member + score)
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_zrangebylex_empty_set() {
    let ctx = TestContext::new().await;

    // Get range by lex from empty set
    let result = ctx.zrangebylex("myzset", "[a", "[z", None).await.unwrap();
    assert_eq!(result, RespValue::Array(vec![]));
}

#[tokio::test]
async fn test_zremrangebylex_empty_set() {
    let ctx = TestContext::new().await;

    // Remove range by lex from empty set
    let result = ctx.zremrangebylex("myzset", "[a", "[z").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zrangestore_empty_set() {
    let ctx = TestContext::new().await;

    // Store range from empty set
    let result = ctx
        .zrangestore("destination", "source", 0, -1, false, false)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));

    // Verify destination doesn't exist
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zrangestore_out_of_bounds() {
    let ctx = TestContext::new().await;

    // Add members
    ctx.zadd("source", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Store range out of bounds
    let result = ctx
        .zrangestore("destination", "source", 10, 20, false, false)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(0));
}

#[tokio::test]
async fn test_zunionstore_overwrite_destination() {
    let ctx = TestContext::new().await;

    // Create destination with existing data
    ctx.zadd("destination", &[("0.0", "old1"), ("0.0", "old2")], &[])
        .await
        .unwrap();

    // Create source sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();

    // Store union (should overwrite destination)
    let result = ctx
        .zunionstore("destination", &["zset1"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify destination overwritten
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_zinterstore_overwrite_destination() {
    let ctx = TestContext::new().await;

    // Create destination with existing data
    ctx.zadd("destination", &[("0.0", "old1"), ("0.0", "old2")], &[])
        .await
        .unwrap();

    // Create source sets
    ctx.zadd("zset1", &[("1.0", "member1")], &[]).await.unwrap();
    ctx.zadd("zset2", &[("2.0", "member1")], &[]).await.unwrap();

    // Store intersection (should overwrite destination)
    let result = ctx
        .zinterstore("destination", &["zset1", "zset2"], None, None)
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify destination overwritten
    let result = ctx.zcard("destination").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
}

#[tokio::test]
async fn test_zadd_multiple_with_mixed_options() {
    let ctx = TestContext::new().await;

    // Add multiple members with options
    let result = ctx
        .zadd(
            "myzset",
            &[("1.0", "member1"), ("2.0", "member2"), ("3.0", "member3")],
            &["CH"],
        )
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(3)); // 3 changed (all new)

    // Update some with CH
    let result = ctx
        .zadd("myzset", &[("4.0", "member1"), ("5.0", "member4")], &["CH"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::Integer(2)); // 2 changed (1 updated, 1 new)
}

#[tokio::test]
async fn test_zmscore_all_nonexistent() {
    let ctx = TestContext::new().await;

    // Get scores for non-existent members
    let result = ctx
        .zmscore("myzset", &["member1", "member2"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::Null);
            assert_eq!(values[1], RespValue::Null);
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_zmscore_mixed_existing_nonexistent() {
    let ctx = TestContext::new().await;

    // Add some members
    ctx.zadd("myzset", &[("1.0", "member1"), ("2.0", "member2")], &[])
        .await
        .unwrap();

    // Get scores for mixed existing and non-existent
    let result = ctx
        .zmscore("myzset", &["member1", "member3", "member2"])
        .await
        .unwrap();
    match result {
        RespValue::Array(values) => {
            assert_eq!(values.len(), 3);
            match &values[0] {
                RespValue::BulkString(bs) => assert_eq!(String::from_utf8_lossy(bs), "1"),
                _ => panic!("Expected BulkString"),
            }
            assert_eq!(values[1], RespValue::Null);
            match &values[2] {
                RespValue::BulkString(bs) => assert_eq!(String::from_utf8_lossy(bs), "2"),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected array response"),
    }
}
