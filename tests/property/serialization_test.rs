// tests/property/serialization_test.rs

//! Property-based tests for serialization/deserialization
//! Tests that data can be serialized and deserialized correctly

use crate::test_helpers::TestContext;
use proptest::prelude::*;
use spineldb::core::RespValue;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 30, // Fewer cases for serialization tests as they may be slower
        max_shrink_iters: 300,
        ..ProptestConfig::default()
    })]

    #[test]
    fn test_string_serialization_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        value in ".{0,1000}"
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // SET value
            ctx.set(&key, &value).await.unwrap();

            // GET and verify
            let get_result = ctx.get(&key).await.unwrap();
            match get_result {
                RespValue::BulkString(bs) => {
                    assert_eq!(String::from_utf8_lossy(&bs), value);
                }
                _ => panic!("GET should return BulkString"),
            }
        });
    }

    #[test]
    fn test_hash_serialization_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        fields in prop::collection::hash_map(
            "[a-zA-Z0-9_]{1,50}",
            ".{0,500}",
            1..=10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // HSET all fields
            let pairs: Vec<(&str, &str)> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            ctx.hset(&key, &pairs).await.unwrap();

            // Retrieve all fields and verify
            let hgetall_result = ctx.hgetall(&key).await.unwrap();
            match hgetall_result {
                RespValue::Array(arr) => {
                    assert_eq!(arr.len() % 2, 0);
                    assert_eq!(arr.len() / 2, fields.len());

                    let mut retrieved_fields = std::collections::HashMap::new();
                    for chunk in arr.chunks(2) {
                        if let (RespValue::BulkString(f), RespValue::BulkString(v)) = (&chunk[0], &chunk[1]) {
                            let field_str = String::from_utf8_lossy(f).to_string();
                            let value_str = String::from_utf8_lossy(v).to_string();
                            retrieved_fields.insert(field_str, value_str);
                        }
                    }

                    assert_eq!(retrieved_fields, fields);
                }
                _ => panic!("HGETALL should return Array"),
            }
        });
    }

    #[test]
    fn test_numeric_serialization_consistency(
        key in "[a-zA-Z0-9_]{1,100}",
        numbers in prop::collection::vec(-10000i64..=10000i64, 1..=20)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;

            // SET each number and verify it can be retrieved correctly
            for num in &numbers {
                let num_str = num.to_string();
                ctx.set(&key, &num_str).await.unwrap();

                let get_result = ctx.get(&key).await.unwrap();
                match get_result {
                    RespValue::BulkString(bs) => {
                        let retrieved: i64 = String::from_utf8_lossy(&bs).parse().unwrap();
                        assert_eq!(retrieved, *num);
                    }
                    _ => panic!("GET should return BulkString"),
                }
            }
        });
    }
}
