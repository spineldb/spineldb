// Integration test for HyperLogLog commands
use bytes::Bytes;
use spineldb::core::commands::hyperloglog::{PfAdd, PfCount, PfMerge};
use spineldb::core::database::Db;
use spineldb::core::database::ExecutionContext;
use spineldb::core::storage::data_types::DataValue;
use spineldb::core::{Command, RespValue};
use std::sync::Arc;

#[cfg(test)]
mod hyperloglog_tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_pfadd_command() {
        let db = Db::new();
        let mut ctx = ExecutionContext::new(Arc::new(db), 0, None);

        let key = Bytes::from("hll_key_pfadd");
        let elements = vec![Bytes::from("element1"), Bytes::from("element2")];

        let cmd = PfAdd {
            key: key.clone(),
            elements,
        };

        // Execute PFADD command
        let (result, _) = cmd.execute(&mut ctx).await.unwrap();

        // Should return 1 (changed) since we added new elements
        assert_eq!(result, RespValue::Integer(1));

        // Verify the HyperLogLog was created and has the correct count
        let shard_index = ctx.db.get_shard_index(&key);
        let guard = ctx.db.get_shard(shard_index).entries.lock().await;
        let entry = guard.peek(&key).unwrap();

        match &entry.data {
            DataValue::HyperLogLog(hll) => {
                // For small cardinalities, the count should be exact
                assert_eq!(hll.count(), 2);
            }
            _ => panic!("Expected HyperLogLog type"),
        }
    }

    #[tokio::test]
    async fn test_pfcount_single_key() {
        let db = Db::new();
        let mut ctx = ExecutionContext::new(Arc::new(db), 0, None);

        let key = Bytes::from("hll_key_pfcount_single");
        let elements = vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")];

        let cmd = PfAdd {
            key: key.clone(),
            elements,
        };
        cmd.execute(&mut ctx).await.unwrap();

        // Count the elements
        let cmd = PfCount { keys: vec![key] };
        let (result, _) = cmd.execute(&mut ctx).await.unwrap();

        // For small N, count should be exact
        assert_eq!(result, RespValue::Integer(3));
    }

    #[tokio::test]
    async fn test_pfcount_multiple_keys() {
        let db = Db::new();
        let mut ctx = ExecutionContext::new(Arc::new(db), 0, None);

        let key1 = Bytes::from("hll_key_multi1");
        let elements1 = vec![Bytes::from("a"), Bytes::from("b")];
        let cmd = PfAdd {
            key: key1.clone(),
            elements: elements1,
        };
        cmd.execute(&mut ctx).await.unwrap();

        let key2 = Bytes::from("hll_key_multi2");
        let elements2 = vec![Bytes::from("c"), Bytes::from("d")];
        let cmd = PfAdd {
            key: key2.clone(),
            elements: elements2,
        };
        cmd.execute(&mut ctx).await.unwrap();

        // Count the elements across both HyperLogLogs (union of disjoint sets)
        let cmd = PfCount {
            keys: vec![key1, key2],
        };
        let (result, _) = cmd.execute(&mut ctx).await.unwrap();

        assert_eq!(result, RespValue::Integer(4));
    }

    #[tokio::test]
    async fn test_pfmerge() {
        let db = Db::new();
        let mut ctx = ExecutionContext::new(Arc::new(db), 0, None);

        let key1 = Bytes::from("hll_key_merge1");
        let elements1 = vec![Bytes::from("a"), Bytes::from("b")];
        let cmd = PfAdd {
            key: key1.clone(),
            elements: elements1,
        };
        cmd.execute(&mut ctx).await.unwrap();

        let key2 = Bytes::from("hll_key_merge2");
        let elements2 = vec![Bytes::from("c"), Bytes::from("d"), Bytes::from("a")]; // "a" is overlapping
        let cmd = PfAdd {
            key: key2.clone(),
            elements: elements2,
        };
        cmd.execute(&mut ctx).await.unwrap();

        let dest_key = Bytes::from("hll_merged");
        let cmd = PfMerge {
            dest_key: dest_key.clone(),
            source_keys: vec![key1, key2],
        };
        let (result, _) = cmd.execute(&mut ctx).await.unwrap();

        assert_eq!(result, RespValue::SimpleString("OK".into()));

        // Verify the merged result. The union of {a, b} and {c, d, a} is {a, b, c, d}.
        let cmd = PfCount {
            keys: vec![dest_key],
        };
        let (result, _) = cmd.execute(&mut ctx).await.unwrap();

        assert_eq!(result, RespValue::Integer(4));
    }

    #[tokio::test]
    async fn test_pfadd_and_verify_state() {
        let db = Db::new();
        let mut ctx = ExecutionContext::new(Arc::new(db), 0, None);

        let key = Bytes::from("hll_verify_state");
        let elements = vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")];

        let cmd = PfAdd {
            key: key.clone(),
            elements,
        };
        cmd.execute(&mut ctx).await.unwrap();

        // Get the stored value and check its type and cardinality
        let shard_index = ctx.db.get_shard_index(&key);
        let guard = ctx.db.get_shard(shard_index).entries.lock().await;
        let stored_value = guard.peek(&key).unwrap();

        assert!(matches!(stored_value.data, DataValue::HyperLogLog(_)));

        match &stored_value.data {
            DataValue::HyperLogLog(hll) => {
                assert_eq!(hll.count(), 3);
            }
            _ => panic!("Expected HyperLogLog"),
        }
    }

    #[tokio::test]
    async fn test_pfcount_estimation_accuracy() {
        let db = Db::new();
        let mut ctx = ExecutionContext::new(Arc::new(db), 0, None);
        let key = Bytes::from("hll_estimation");
        let num_elements = 1000;

        // PFADD in chunks
        for i in (0..num_elements).step_by(100) {
            let elements: Vec<Bytes> = (i..i + 100)
                .map(|n| Bytes::from(format!("element-{}", n)))
                .collect();
            let cmd = PfAdd {
                key: key.clone(),
                elements,
            };
            cmd.execute(&mut ctx).await.unwrap();
        }

        let cmd = PfCount { keys: vec![key] };
        let (result, _) = cmd.execute(&mut ctx).await.unwrap();

        if let RespValue::Integer(count) = result {
            let error_margin = (num_elements as f64 * 0.15) as i64; // 15% margin for integration test
            let lower_bound = num_elements - error_margin;
            let upper_bound = num_elements + error_margin;
            assert!(
                count >= lower_bound && count <= upper_bound,
                "Count {} should be within 15% of {}",
                count,
                num_elements
            );
        } else {
            panic!("Expected integer response from PFCOUNT");
        }
    }
}
