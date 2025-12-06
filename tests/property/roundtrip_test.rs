// tests/property/roundtrip_test.rs

//! Property-based tests for roundtrip operations
//! Tests that SET/GET, HSET/HGET, and other write/read operations preserve data correctly

use crate::test_helpers::TestContext;
use proptest::prelude::*;
use spineldb::core::RespValue;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 100,
        max_shrink_iters: 1000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn test_set_get_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        value in ".{0,10000}"
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // SET the value
            let set_result = ctx.set(&key, &value).await.unwrap();
            assert_eq!(set_result, RespValue::SimpleString("OK".into()));

            // GET the value back
            let get_result = ctx.get(&key).await.unwrap();
            match get_result {
                RespValue::BulkString(bs) => {
                    assert_eq!(String::from_utf8_lossy(&bs), value);
                }
                _ => panic!("GET should return BulkString, got {:?}", get_result),
            }
        });
    }

    #[test]
    fn test_hset_hget_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        field in "[a-zA-Z0-9_]{1,100}",
        value in ".{0,10000}"
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // HSET the value
            let hset_result = ctx.hset(&key, &[(&field, &value)]).await.unwrap();
            assert_eq!(hset_result, RespValue::Integer(1));

            // HGET the value back
            let hget_result = ctx.hget(&key, &field).await.unwrap();
            match hget_result {
                RespValue::BulkString(bs) => {
                    assert_eq!(String::from_utf8_lossy(&bs), value);
                }
                _ => panic!("HGET should return BulkString, got {:?}", hget_result),
            }
        });
    }

    #[test]
    fn test_hset_hgetall_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        fields in prop::collection::hash_map(
            "[a-zA-Z0-9_]{1,50}",
            ".{0,1000}",
            1..=20
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // Convert HashMap to slice of tuples for HSET
            let field_value_pairs: Vec<(&str, &str)> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            // HSET multiple fields
            let hset_result = ctx.hset(&key, &field_value_pairs).await.unwrap();
            assert!(matches!(hset_result, RespValue::Integer(_)));

            // HGETALL to retrieve all fields
            let hgetall_result = ctx.hgetall(&key).await.unwrap();
            match hgetall_result {
                RespValue::Array(arr) => {
                    // Array should have even number of elements (field-value pairs)
                    assert_eq!(arr.len() % 2, 0);
                    assert_eq!(arr.len() / 2, fields.len());

                    // Verify all fields are present
                    let mut retrieved_fields = std::collections::HashMap::new();
                    for chunk in arr.chunks(2) {
                        if let (RespValue::BulkString(f), RespValue::BulkString(v)) = (&chunk[0], &chunk[1]) {
                            let field_str = String::from_utf8_lossy(f);
                            let value_str = String::from_utf8_lossy(v);
                            retrieved_fields.insert(field_str.to_string(), value_str.to_string());
                        }
                    }

                    // Check all original fields are present with correct values
                    for (field, value) in &fields {
                        assert_eq!(retrieved_fields.get(field), Some(value));
                    }
                }
                _ => panic!("HGETALL should return an array"),
            }
        });
    }

    #[test]
    fn test_rpush_lrange_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        values in prop::collection::vec(".{0,1000}", 1..=50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // Convert Vec<String> to &[&str]
            let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

            // RPUSH all values
            let rpush_result = ctx.rpush(&key, &value_refs).await.unwrap();
            assert_eq!(rpush_result, RespValue::Integer(values.len() as i64));

            // LRANGE to get all values back (in same order due to RPUSH)
            let lrange_result = ctx.lrange(&key, 0, -1).await.unwrap();
            match lrange_result {
                RespValue::Array(arr) => {
                    assert_eq!(arr.len(), values.len());

                    // RPUSH adds to end, so order is preserved
                    for (i, value) in values.iter().enumerate() {
                        if let RespValue::BulkString(bs) = &arr[i] {
                            assert_eq!(String::from_utf8_lossy(&bs), *value);
                        } else {
                            panic!("LRANGE should return BulkString elements");
                        }
                    }
                }
                _ => panic!("LRANGE should return Array"),
            }
        });
    }

    #[test]
    fn test_sadd_smembers_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        members in prop::collection::hash_set(".{0,1000}", 1..=50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // Convert HashSet to Vec<&str>
            let member_refs: Vec<&str> = members.iter().map(|s| s.as_str()).collect();

            // SADD all members
            let sadd_result = ctx.sadd(&key, &member_refs).await.unwrap();
            assert_eq!(sadd_result, RespValue::Integer(members.len() as i64));

            // SMEMBERS to get all members back
            let smembers_result = ctx.smembers(&key).await.unwrap();
            match smembers_result {
                RespValue::Array(arr) => {
                    assert_eq!(arr.len(), members.len());

                    // Convert array to HashSet for comparison
                    let mut retrieved_members = std::collections::HashSet::new();
                    for item in &arr {
                        if let RespValue::BulkString(bs) = item {
                            retrieved_members.insert(String::from_utf8_lossy(bs).to_string());
                        }
                    }

                    // Verify all original members are present
                    assert_eq!(retrieved_members, members);
                }
                _ => panic!("SMEMBERS should return Array"),
            }
        });
    }
}
