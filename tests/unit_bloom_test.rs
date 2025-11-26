// tests/unit_bloom_test.rs

//! Integration tests for Bloom Filter commands

use bytes::Bytes;
use spineldb::core::commands::command_trait::CommandExt;
use spineldb::core::protocol::RespFrame;
use spineldb::core::{Command, RespValue, SpinelDBError};

// It's common to put test helpers in a submodule or a separate file.
// For this test, we'll include the necessary helper setup directly.
mod bloom_test_helpers {
    use super::*;
    use spineldb::config::Config;
    use spineldb::core::database::context::ExecutionContext;
    use spineldb::core::database::core::Db;
    use spineldb::core::state::ServerState;
    use std::sync::Arc;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, reload};

    pub struct TestContext {
        pub state: Arc<ServerState>,
        pub db: Arc<Db>,
    }

    impl TestContext {
        pub async fn new() -> Self {
            let mut config = Config::default();
            config.databases = 1;
            config.persistence.aof_enabled = false;
            config.persistence.spldb_enabled = false;

            let env_filter = EnvFilter::new("warn");
            let (filter, reload_handle) = reload::Layer::new(env_filter);
            let _ = tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().with_test_writer())
                .try_init();
            let reload_handle = Arc::new(reload_handle);
            let server_init = ServerState::initialize(config, reload_handle).unwrap();
            let state = server_init.state;
            let db = state.get_db(0).unwrap();
            Self { state, db }
        }

        pub async fn execute(&self, command: Command) -> Result<RespValue, SpinelDBError> {
            let locks = self.db.determine_locks_for_command(&command).await;
            let mut ctx = ExecutionContext {
                state: self.state.clone(),
                locks,
                db: &self.db,
                command: Some(command.clone()),
                session_id: 1,
                authenticated_user: None,
            };
            let (resp, _outcome) = command.execute(&mut ctx).await?;
            Ok(resp)
        }

        pub async fn command_from_frames(
            &self,
            frames: Vec<RespFrame>,
        ) -> Result<RespValue, SpinelDBError> {
            let command = Command::try_from(RespFrame::Array(frames))?;
            self.execute(command).await
        }

        pub async fn bf_reserve(
            &self,
            key: &str,
            error_rate: f64,
            capacity: u64,
        ) -> Result<RespValue, SpinelDBError> {
            self.command_from_frames(vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"RESERVE")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
                RespFrame::BulkString(Bytes::from(error_rate.to_string())),
                RespFrame::BulkString(Bytes::from(capacity.to_string())),
            ])
            .await
        }

        pub async fn bf_add(&self, key: &str, item: &str) -> Result<RespValue, SpinelDBError> {
            self.command_from_frames(vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"ADD")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
                RespFrame::BulkString(Bytes::from(item.to_string())),
            ])
            .await
        }

        pub async fn bf_madd(&self, key: &str, items: &[&str]) -> Result<RespValue, SpinelDBError> {
            let mut frames = vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"MADD")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
            ];
            for item in items {
                frames.push(RespFrame::BulkString(Bytes::from(item.to_string())));
            }
            self.command_from_frames(frames).await
        }

        pub async fn bf_exists(&self, key: &str, item: &str) -> Result<RespValue, SpinelDBError> {
            self.command_from_frames(vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"EXISTS")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
                RespFrame::BulkString(Bytes::from(item.to_string())),
            ])
            .await
        }

        pub async fn bf_mexists(
            &self,
            key: &str,
            items: &[&str],
        ) -> Result<RespValue, SpinelDBError> {
            let mut frames = vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"MEXISTS")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
            ];
            for item in items {
                frames.push(RespFrame::BulkString(Bytes::from(item.to_string())));
            }
            self.command_from_frames(frames).await
        }

        pub async fn bf_card(&self, key: &str) -> Result<RespValue, SpinelDBError> {
            self.command_from_frames(vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"CARD")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
            ])
            .await
        }

        pub async fn bf_info(&self, key: &str) -> Result<RespValue, SpinelDBError> {
            self.command_from_frames(vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"INFO")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
            ])
            .await
        }

        pub async fn bf_insert(
            &self,
            key: &str,
            options: &[&str],
            items: &[&str],
        ) -> Result<RespValue, SpinelDBError> {
            let mut frames = vec![
                RespFrame::BulkString(Bytes::from_static(b"BF")),
                RespFrame::BulkString(Bytes::from_static(b"INSERT")),
                RespFrame::BulkString(Bytes::from(key.to_string())),
            ];
            for option in options {
                frames.push(RespFrame::BulkString(Bytes::from(option.to_string())));
            }
            frames.push(RespFrame::BulkString(Bytes::from_static(b"ITEMS")));
            for item in items {
                frames.push(RespFrame::BulkString(Bytes::from(item.to_string())));
            }
            self.command_from_frames(frames).await
        }
    }
}

use bloom_test_helpers::TestContext;

#[tokio::test]
async fn test_bf_reserve_and_add_and_exists() {
    let ctx = TestContext::new().await;
    let key = "bf_test_1";

    // 1. Reserve a filter
    let res = ctx.bf_reserve(key, 0.01, 1000).await.unwrap();
    assert_eq!(res, RespValue::SimpleString("OK".into()));

    // 2. Add an item
    let res = ctx.bf_add(key, "item1").await.unwrap();
    assert_eq!(res, RespValue::Integer(1)); // 1 means item was added

    // 3. Check if it exists
    let res = ctx.bf_exists(key, "item1").await.unwrap();
    assert_eq!(res, RespValue::Integer(1)); // 1 means item may exist

    // 4. Check for a non-existent item
    let res = ctx.bf_exists(key, "item2").await.unwrap();
    assert_eq!(res, RespValue::Integer(0)); // 0 means item definitely does not exist

    // 5. Add the same item again
    let res = ctx.bf_add(key, "item1").await.unwrap();
    assert_eq!(res, RespValue::Integer(0)); // 0 means item was already present
}

#[tokio::test]
async fn test_bf_madd_and_mexists() {
    let ctx = TestContext::new().await;
    let key = "bf_test_2";

    // 1. Reserve a filter
    ctx.bf_reserve(key, 0.01, 1000).await.unwrap();

    // 2. Add multiple items
    let res = ctx.bf_madd(key, &["a", "b", "c"]).await.unwrap();
    assert_eq!(
        res,
        RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(1),
            RespValue::Integer(1)
        ])
    );

    // 3. Check multiple items
    let res = ctx.bf_mexists(key, &["a", "d", "c"]).await.unwrap();
    assert_eq!(
        res,
        RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(0),
            RespValue::Integer(1)
        ])
    );

    // 4. Add same and new items
    let res = ctx.bf_madd(key, &["a", "d"]).await.unwrap();
    assert_eq!(
        res,
        RespValue::Array(vec![RespValue::Integer(0), RespValue::Integer(1)])
    );
}

#[tokio::test]
async fn test_bf_card_and_info() {
    let ctx = TestContext::new().await;
    let key = "bf_test_3";

    // 1. Card on non-existent key
    let res = ctx.bf_card(key).await.unwrap();
    assert_eq!(res, RespValue::Integer(0));

    // 2. Reserve and check info
    ctx.bf_reserve(key, 0.01, 1000).await.unwrap();
    let res = ctx.bf_info(key).await.unwrap();
    let info = match res {
        RespValue::Array(arr) => arr,
        _ => panic!("Expected array response from BF.INFO"),
    };
    assert_eq!(info[1], RespValue::Integer(1000)); // Capacity
    assert_eq!(info[7], RespValue::Integer(0)); // Items inserted

    // 3. Add items and check card
    ctx.bf_add(key, "item1").await.unwrap();
    ctx.bf_add(key, "item2").await.unwrap();
    let res = ctx.bf_card(key).await.unwrap();
    assert_eq!(res, RespValue::Integer(2));

    // 4. Add existing item and check card does not change
    ctx.bf_add(key, "item1").await.unwrap();
    let res = ctx.bf_card(key).await.unwrap();
    assert_eq!(res, RespValue::Integer(2));

    // 5. Check info again
    let res = ctx.bf_info(key).await.unwrap();
    let info = match res {
        RespValue::Array(arr) => arr,
        _ => panic!("Expected array response from BF.INFO"),
    };
    assert_eq!(info[7], RespValue::Integer(2)); // Items inserted
}

#[tokio::test]
async fn test_bf_insert() {
    let ctx = TestContext::new().await;
    let key = "bf_test_4";

    // 1. Insert into a new key with custom params
    let res = ctx
        .bf_insert(
            key,
            &["CAPACITY", "500", "ERROR", "0.05"],
            &["item1", "item2"],
        )
        .await
        .unwrap();
    assert_eq!(
        res,
        RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(1)])
    );

    // 2. Check info
    let res = ctx.bf_info(key).await.unwrap();
    let info = match res {
        RespValue::Array(arr) => arr,
        _ => panic!("Expected array response from BF.INFO"),
    };
    assert_eq!(info[1], RespValue::Integer(500)); // Capacity
    assert_eq!(info[7], RespValue::Integer(2)); // Items inserted

    // 3. Insert more items
    let res = ctx.bf_insert(key, &[], &["item3", "item1"]).await.unwrap();
    assert_eq!(
        res,
        RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(0)])
    );

    // 4. Check card
    let res = ctx.bf_card(key).await.unwrap();
    assert_eq!(res, RespValue::Integer(3));

    // 5. Try to insert with params on existing filter
    let err = ctx
        .bf_insert(key, &["CAPACITY", "1000"], &["item4"])
        .await
        .unwrap_err();
    assert!(matches!(err, SpinelDBError::InvalidRequest(_)));
}

#[tokio::test]
async fn test_bf_auto_creation() {
    let ctx = TestContext::new().await;
    let key = "bf_test_5";

    // BF.ADD on a non-existent key should create it
    let res = ctx.bf_add(key, "item1").await.unwrap();
    assert_eq!(res, RespValue::Integer(1));

    // Check info to see default parameters
    let res = ctx.bf_info(key).await.unwrap();
    let info = match res {
        RespValue::Array(arr) => arr,
        _ => panic!("Expected array response from BF.INFO"),
    };
    assert_eq!(info[1], RespValue::Integer(100)); // Default capacity
    assert_eq!(info[7], RespValue::Integer(1)); // Items inserted
}
