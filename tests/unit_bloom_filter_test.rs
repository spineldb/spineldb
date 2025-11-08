// tests/unit_bloom_filter_test.rs

#[cfg(test)]
mod unit_bloom_filter_test {
    use bytes::Bytes;
    use spineldb::core::commands::bloom::bf_add::BfAdd;
    use spineldb::core::commands::bloom::bf_exists::BfExists;
    use spineldb::core::commands::bloom::bf_reserve::BfReserve;
    use spineldb::core::commands::command_trait::ExecutableCommand;
    use spineldb::core::commands::string::Set;
    use spineldb::core::database::{Database, ExecutionContext};
    use spineldb::core::{RespValue, SpinelDBError, WriteOutcome};
    use std::sync::Arc;

    async fn execute_command(
        ctx: &mut ExecutionContext<'_>,
        cmd: impl ExecutableCommand,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        cmd.execute(ctx).await
    }

    #[tokio::test]
    async fn test_bf_reserve_and_add_and_exists() {
        let db = Arc::new(Database::new(None));
        let mut ctx = ExecutionContext::new(db);

        let key = Bytes::from("mybloom");
        let item1 = Bytes::from("hello");
        let item2 = Bytes::from("world");

        // 1. Reserve the filter
        let reserve_cmd = BfReserve {
            key: key.clone(),
            error_rate: 0.01,
            capacity: 1000,
        };
        let (resp, outcome) = execute_command(&mut ctx, reserve_cmd).await.unwrap();
        assert_eq!(resp, RespValue::SimpleString("OK".into()));
        assert_eq!(outcome, WriteOutcome::Write { keys_modified: 1 });

        // 2. Try to reserve again, should fail
        let reserve_cmd_fail = BfReserve {
            key: key.clone(),
            error_rate: 0.01,
            capacity: 1000,
        };
        let result = execute_command(&mut ctx, reserve_cmd_fail).await;
        assert!(matches!(result, Err(SpinelDBError::KeyExists)));

        // 3. Add item1 to the filter
        let add_cmd_1 = BfAdd {
            key: key.clone(),
            item: item1.clone(),
        };
        let (resp, outcome) = execute_command(&mut ctx, add_cmd_1).await.unwrap();
        assert_eq!(resp, RespValue::Integer(1)); // 1 indicates item was added (bit was flipped)
        assert_eq!(outcome, WriteOutcome::Write { keys_modified: 1 });

        // 4. Add item1 again, should return 0
        let add_cmd_1_again = BfAdd {
            key: key.clone(),
            item: item1.clone(),
        };
        let (resp, outcome) = execute_command(&mut ctx, add_cmd_1_again).await.unwrap();
        assert_eq!(resp, RespValue::Integer(0)); // 0 indicates item was already present
        assert_eq!(outcome, WriteOutcome::DidNotWrite);

        // 5. Check for existence of item1
        let exists_cmd_1 = BfExists {
            key: key.clone(),
            item: item1.clone(),
        };
        let (resp, _) = execute_command(&mut ctx, exists_cmd_1).await.unwrap();
        assert_eq!(resp, RespValue::Integer(1)); // 1 means exists

        // 6. Check for existence of item2 (not added yet)
        let exists_cmd_2 = BfExists {
            key: key.clone(),
            item: item2.clone(),
        };
        let (resp, _) = execute_command(&mut ctx, exists_cmd_2).await.unwrap();
        assert_eq!(resp, RespValue::Integer(0)); // 0 means does not exist
    }

    #[tokio::test]
    async fn test_bf_add_creates_default_filter() {
        let db = Arc::new(Database::new(None));
        let mut ctx = ExecutionContext::new(db);

        let key = Bytes::from("default_bloom");
        let item = Bytes::from("some_item");

        // Add an item to a non-existent key, should create a default filter
        let add_cmd = BfAdd {
            key: key.clone(),
            item: item.clone(),
        };
        let (resp, _) = execute_command(&mut ctx, add_cmd).await.unwrap();
        assert_eq!(resp, RespValue::Integer(1));

        // Check if the item exists
        let exists_cmd = BfExists {
            key: key.clone(),
            item: item.clone(),
        };
        let (resp, _) = execute_command(&mut ctx, exists_cmd).await.unwrap();
        assert_eq!(resp, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_bloom_filter_wrong_type() {
        let db = Arc::new(Database::new(None));
        let mut ctx = ExecutionContext::new(db);

        let key = Bytes::from("not_a_bloom");

        // Set a string value at the key
        let set_cmd = Set {
            key: key.clone(),
            value: Bytes::from("some string"),
            ttl: Default::default(),
            condition: Default::default(),
            get: false,
        };
        execute_command(&mut ctx, set_cmd).await.unwrap();

        // Try to use a bloom filter command on the string key
        let add_cmd = BfAdd {
            key: key.clone(),
            item: Bytes::from("test"),
        };
        let result = execute_command(&mut ctx, add_cmd).await;
        assert!(matches!(result, Err(SpinelDBError::WrongType)));
    }
}
