use crate::integration::test_helpers::TestContext;
use bytes::Bytes;
use spineldb::config::Config;
use spineldb::core::RespValue;
use spineldb::core::commands::command_trait::CommandExt;
use spineldb::core::protocol::RespFrame;
use std::time::Duration;
use tempfile::tempdir;

// Helper to create a PFADD command
fn pfadd_cmd(key: &str, elements: &[&str]) -> RespFrame {
    let mut args = vec![
        RespFrame::BulkString(Bytes::from_static(b"PFADD")),
        RespFrame::BulkString(Bytes::from(key.to_string())),
    ];
    for element in elements {
        args.push(RespFrame::BulkString(Bytes::from(element.to_string())));
    }
    RespFrame::Array(args)
}

// Helper to create a PFCOUNT command
fn pfcount_cmd(key: &str) -> RespFrame {
    RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"PFCOUNT")),
        RespFrame::BulkString(Bytes::from(key.to_string())),
    ])
}

// Helper to create a BGREREWRITEAOF command
fn bgrerewriteaof_cmd() -> RespFrame {
    RespFrame::Array(vec![RespFrame::BulkString(Bytes::from_static(
        b"BGREREWRITEAOF",
    ))])
}

#[tokio::test]
async fn test_hyperloglog_aof_persistence() {
    // 1. Setup a temporary directory for persistence files
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let aof_path = temp_dir.path().to_path_buf();

    // --- First Instance: Write data and trigger AOF rewrite ---
    {
        // 2. Configure the first server instance with AOF enabled
        let mut config = Config::default();
        config.databases = 1;
        config.persistence.aof_enabled = true;
        config.persistence.dir = aof_path.to_str().unwrap().to_string();
        config.persistence.aof_filename = "spinel.aof".to_string();
        // Disable AOF fsync to speed up tests
        config.persistence.aof_fsync_strategy = "never".to_string();

        let ctx = TestContext::with_config(config).await;

        // 3. PFADD some data to an HLL
        let cmd = pfadd_cmd("hll_key", &["a", "b", "c", "a", "d"]);
        let res = ctx.execute_frame(cmd).await.unwrap();
        assert_eq!(res, RespValue::Integer(1)); // 1 indicates the HLL was modified

        // 4. Trigger BGREREWRITEAOF to persist the HLL as a SET command
        let cmd = bgrerewriteaof_cmd();
        let res = ctx.execute_frame(cmd).await.unwrap();
        assert_eq!(res, RespValue::SimpleString("OK".into()));

        // Give some time for the rewrite to complete
        tokio::time::sleep(Duration::from_millis(100)).await;
    } // The first context is dropped here, simulating a server shutdown

    // --- Second Instance: Load from AOF and verify data ---
    {
        // 5. Configure the second server instance to load from the same AOF file
        let mut config = Config::default();
        config.databases = 1;
        config.persistence.aof_enabled = true;
        config.persistence.dir = aof_path.to_str().unwrap().to_string();
        config.persistence.aof_filename = "spinel.aof".to_string();

        // This context will load from the AOF file on initialization
        let ctx = TestContext::with_config(config).await;

        // 6. PFCOUNT the key to see if it was restored
        let cmd = pfcount_cmd("hll_key");
        let res = ctx.execute_frame(cmd).await.unwrap();

        // The HLL had 4 unique elements: "a", "b", "c", "d"
        // The count should be 4.
        assert_eq!(res, RespValue::Integer(4));
    }

    // The temp_dir is automatically cleaned up when it goes out of scope
}
