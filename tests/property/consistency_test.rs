// tests/property/consistency_test.rs

//! Property-based tests for data consistency
//! Tests that operations maintain consistency invariants

use crate::test_helpers::TestContext;
use proptest::prelude::*;
use spineldb::core::RespValue;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 50, // Fewer cases for consistency tests
        max_shrink_iters: 500,
        ..ProptestConfig::default()
    })]

    #[test]
    fn test_set_get_consistency_multiple_keys(
        key_value_pairs in prop::collection::hash_map(
            "[a-zA-Z0-9_]{1,100}",
            ".{0,1000}",
            1..=50
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // SET all key-value pairs
            for (key, value) in &key_value_pairs {
                let set_result = ctx.set(key, value).await.unwrap();
                assert_eq!(set_result, RespValue::SimpleString("OK".into()));
            }

            // Verify all keys can be retrieved with correct values
            for (key, expected_value) in &key_value_pairs {
                let get_result = ctx.get(key).await.unwrap();
                match get_result {
                    RespValue::BulkString(bs) => {
                        assert_eq!(String::from_utf8_lossy(&bs), *expected_value);
                    }
                    _ => panic!("GET should return BulkString for key '{}'", key),
                }
            }
        });
    }

    #[test]
    fn test_list_length_consistency(
        key in "[a-zA-Z0-9_]{1,100}",
        operations in prop::collection::vec(
            prop::sample::select(vec!["LPUSH", "RPUSH", "LPOP", "RPOP"]),
            1..=50
        ),
        values in prop::collection::vec(".{0,100}", 1..=50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            let mut expected_length = 0;
            let mut value_index = 0;

            // Apply operations and track expected length
            for op in &operations {
                match op.as_ref() {
                    "LPUSH" => {
                        if value_index < values.len() {
                            let value = &values[value_index];
                            let result = ctx.lpush(&key, &[value]).await.unwrap();
                            assert!(matches!(result, RespValue::Integer(_)));
                            expected_length += 1;
                            value_index += 1;
                        }
                    }
                    "RPUSH" => {
                        if value_index < values.len() {
                            let value = &values[value_index];
                            let result = ctx.rpush(&key, &[value]).await.unwrap();
                            assert!(matches!(result, RespValue::Integer(_)));
                            expected_length += 1;
                            value_index += 1;
                        }
                    }
                    "LPOP" => {
                        if expected_length > 0 {
                            let result = ctx.lpop(&key).await;
                            if result.is_ok() {
                                expected_length -= 1;
                            }
                        }
                    }
                    "RPOP" => {
                        if expected_length > 0 {
                            let result = ctx.rpop(&key).await;
                            if result.is_ok() {
                                expected_length -= 1;
                            }
                        }
                    }
                    _ => unreachable!(),
                }

                // Verify length consistency after each operation
                let llen_result = ctx.llen(&key).await.unwrap();
                match llen_result {
                    RespValue::Integer(len) => {
                        assert_eq!(len, expected_length as i64);
                    }
                    _ => panic!("LLEN should return Integer"),
                }
            }
        });
    }

    #[test]
    fn test_set_cardinality_consistency(
        key in "[a-zA-Z0-9_]{1,100}",
        operations in prop::collection::vec(
            (prop::sample::select(vec!["SADD", "SREM"]), ".{0,100}"),
            1..=50
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            let mut expected_members = std::collections::HashSet::new();

            // Apply all operations
            for (op, member) in &operations {
                match op.as_ref() {
                    "SADD" => {
                        let sadd_result = ctx.sadd(&key, &[member]).await.unwrap();
                        assert!(matches!(sadd_result, RespValue::Integer(_)));
                        expected_members.insert(member.clone());
                    }
                    "SREM" => {
                        let srem_result = ctx.srem(&key, &[member]).await.unwrap();
                        assert!(matches!(srem_result, RespValue::Integer(_)));
                        expected_members.remove(member);
                    }
                    _ => unreachable!(),
                }

                // Verify cardinality consistency
                let scard_result = ctx.scard(&key).await.unwrap();
                match scard_result {
                    RespValue::Integer(card) => {
                        assert_eq!(card, expected_members.len() as i64);
                    }
                    _ => panic!("SCARD should return Integer"),
                }
            }
        });
    }
}
