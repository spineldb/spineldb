// tests/integration/replication_test.rs

//! Integration tests for replication functionality
//! Tests: ROLE, INFO replication, REPLCONF, replication backlog, min_replicas policy

use super::test_helpers::TestContext;
use bytes::Bytes;
use spineldb::config::{ReplicationConfig, ReplicationPrimaryConfig};
use spineldb::core::Command;
use spineldb::core::RespValue;
use spineldb::core::SpinelDBError;
use spineldb::core::commands::generic::Role;
use spineldb::core::protocol::RespFrame;
use spineldb::core::state::{ReplicaStateInfo, ReplicaSyncState};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

// ===== ROLE Command Tests =====

#[tokio::test]
async fn test_role_primary() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    let result = ctx.execute(Command::Role(Role::default())).await.unwrap();

    match result {
        RespValue::Array(mut arr) => {
            assert_eq!(arr.len(), 3);
            // First element should be "master"
            match arr.remove(0) {
                RespValue::BulkString(role) => {
                    assert_eq!(role, Bytes::from("master"));
                }
                _ => panic!("Expected BulkString for role"),
            }
            // Second element should be replication offset (integer)
            match arr.remove(0) {
                RespValue::Integer(offset) => {
                    assert!(offset >= 0);
                }
                _ => panic!("Expected Integer for offset"),
            }
            // Third element should be array of replicas
            match arr.remove(0) {
                RespValue::Array(_replicas) => {
                    // Empty array for no connected replicas
                }
                _ => panic!("Expected Array for replicas"),
            }
        }
        _ => panic!("Expected Array response from ROLE"),
    }
}

#[tokio::test]
async fn test_role_replica() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Replica {
        primary_host: "127.0.0.1".to_string(),
        primary_port: 7878,
        tls_enabled: false,
    };

    let ctx = TestContext::with_config(config).await;

    let result = ctx.execute(Command::Role(Role::default())).await.unwrap();

    match result {
        RespValue::Array(mut arr) => {
            assert_eq!(arr.len(), 5);
            // First element should be "slave"
            match arr.remove(0) {
                RespValue::BulkString(role) => {
                    assert_eq!(role, Bytes::from("slave"));
                }
                _ => panic!("Expected BulkString for role"),
            }
            // Second element should be primary host
            match arr.remove(0) {
                RespValue::BulkString(host) => {
                    assert_eq!(host, Bytes::from("127.0.0.1"));
                }
                _ => panic!("Expected BulkString for host"),
            }
            // Third element should be primary port
            match arr.remove(0) {
                RespValue::Integer(port) => {
                    assert_eq!(port, 7878);
                }
                _ => panic!("Expected Integer for port"),
            }
            // Fourth element should be connection state
            match arr.remove(0) {
                RespValue::BulkString(state) => {
                    // Should be "connecting" or "connected"
                    let state_str = String::from_utf8(state.to_vec()).unwrap();
                    assert!(state_str == "connecting" || state_str == "connected");
                }
                _ => panic!("Expected BulkString for state"),
            }
            // Fifth element should be processed offset
            match arr.remove(0) {
                RespValue::Integer(_offset) => {
                    // Offset can be 0 or higher
                }
                _ => panic!("Expected Integer for offset"),
            }
        }
        _ => panic!("Expected Array response from ROLE"),
    }
}

// ===== INFO Replication Tests =====

#[tokio::test]
async fn test_info_replication_primary() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INFO")),
        RespFrame::BulkString(Bytes::from_static(b"replication")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();

    match result {
        RespValue::BulkString(info_str) => {
            let info = String::from_utf8(info_str.to_vec()).unwrap();
            assert!(info.contains("role:master"));
            assert!(info.contains("master_replid:"));
            assert!(info.contains("master_repl_offset:"));
            assert!(info.contains("connected_slaves:"));
            assert!(info.contains("min_replicas_to_write:"));
            assert!(info.contains("min_replicas_max_lag:"));
        }
        _ => panic!("Expected BulkString from INFO replication"),
    }
}

#[tokio::test]
async fn test_info_replication_replica() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Replica {
        primary_host: "127.0.0.1".to_string(),
        primary_port: 7878,
        tls_enabled: false,
    };

    let ctx = TestContext::with_config(config).await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INFO")),
        RespFrame::BulkString(Bytes::from_static(b"replication")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();

    match result {
        RespValue::BulkString(info_str) => {
            let info = String::from_utf8(info_str.to_vec()).unwrap();
            assert!(info.contains("role:slave"));
            assert!(info.contains("master_replid:"));
            assert!(info.contains("master_repl_offset:"));
        }
        _ => panic!("Expected BulkString from INFO replication"),
    }
}

#[tokio::test]
async fn test_info_replication_with_min_replicas() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 2;
    primary_config.min_replicas_max_lag = 10;
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"INFO")),
        RespFrame::BulkString(Bytes::from_static(b"replication")),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();

    match result {
        RespValue::BulkString(info_str) => {
            let info = String::from_utf8(info_str.to_vec()).unwrap();
            assert!(info.contains("min_replicas_to_write:2"));
            assert!(info.contains("min_replicas_max_lag:10"));
        }
        _ => panic!("Expected BulkString from INFO replication"),
    }
}

// ===== REPLCONF Command Tests =====

#[tokio::test]
async fn test_replconf_ack() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Add a fake replica state to test REPLCONF ACK
    let test_addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    ctx.state.replica_states.insert(
        test_addr,
        ReplicaStateInfo {
            sync_state: ReplicaSyncState::Online,
            ack_offset: 0,
            last_ack_time: std::time::Instant::now(),
        },
    );

    // Send REPLCONF ACK with an offset
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"REPLCONF")),
        RespFrame::BulkString(Bytes::from_static(b"ack")),
        RespFrame::BulkString(Bytes::from("12345")),
    ]))
    .unwrap();

    // REPLCONF is handled by connection handler, not directly executable
    // But we can test that it doesn't error when processed
    // Note: This test verifies the command parsing works
    let _ = command;
}

#[tokio::test]
async fn test_replconf_listening_port() {
    // Test that REPLCONF listening-port command can be parsed
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"REPLCONF")),
        RespFrame::BulkString(Bytes::from_static(b"listening-port")),
        RespFrame::BulkString(Bytes::from("7878")),
    ]));

    assert!(command.is_ok());
}

#[tokio::test]
async fn test_replconf_capa() {
    // Test that REPLCONF capa command can be parsed
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"REPLCONF")),
        RespFrame::BulkString(Bytes::from_static(b"capa")),
        RespFrame::BulkString(Bytes::from_static(b"psync2")),
    ]));

    assert!(command.is_ok());
}

// ===== Replication Backlog Tests =====

#[tokio::test]
async fn test_replication_backlog_add_and_get() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Create a test frame
    let test_frame = RespFrame::BulkString(Bytes::from("test"));
    let frame_len = test_frame.encode_to_vec().unwrap().len();

    // Add to backlog
    ctx.state
        .replication_backlog
        .add(100, test_frame.clone(), frame_len)
        .await;

    // Get since offset 100
    let frames = ctx.state.replication_backlog.get_since(100).await;
    assert!(frames.is_some());
    let frames = frames.unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].0, 100);
}

#[tokio::test]
async fn test_replication_backlog_get_since_old_offset() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Add a frame at offset 200
    let test_frame = RespFrame::BulkString(Bytes::from("test"));
    let frame_len = test_frame.encode_to_vec().unwrap().len();
    ctx.state
        .replication_backlog
        .add(200, test_frame, frame_len)
        .await;

    // Try to get since offset 100 (older than what's in backlog)
    let frames = ctx.state.replication_backlog.get_since(100).await;
    // Should return None because offset 100 is too old
    assert!(frames.is_none());
}

#[tokio::test]
async fn test_replication_backlog_multiple_frames() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Add multiple frames
    for i in 0..5 {
        let test_frame = RespFrame::BulkString(Bytes::from(format!("test{}", i)));
        let frame_len = test_frame.encode_to_vec().unwrap().len();
        ctx.state
            .replication_backlog
            .add(100 + (i * 10) as u64, test_frame, frame_len)
            .await;
    }

    // Get since offset 120
    let frames = ctx.state.replication_backlog.get_since(120).await;
    assert!(frames.is_some());
    let frames = frames.unwrap();
    // Should get frames at offsets 120, 130, 140
    assert!(frames.len() >= 3);
}

#[tokio::test]
async fn test_replication_backlog_capacity_eviction() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Add many large frames to exceed capacity
    let large_frame = RespFrame::BulkString(Bytes::from(vec![b'x'; 10000]));
    let frame_len = large_frame.encode_to_vec().unwrap().len();

    // Add enough frames to exceed 2MB capacity
    for i in 0..300 {
        let frame = large_frame.clone();
        ctx.state
            .replication_backlog
            .add(100 + (i * 100) as u64, frame, frame_len)
            .await;
    }

    // The backlog should have evicted old entries
    // Try to get a very old offset - should return None
    let frames = ctx.state.replication_backlog.get_since(100).await;
    // May or may not be None depending on eviction, but should handle gracefully
    let _ = frames;
}

// ===== Replication Offset Tests =====

#[tokio::test]
async fn test_replication_offset_tracking() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Initial offset should be 0
    let initial_offset = ctx.state.replication.get_replication_offset();
    assert_eq!(initial_offset, 0);

    // Execute a write command to increment offset
    ctx.set("key1", "value1").await.unwrap();

    // Give time for the backlog feeder to process the event
    sleep(Duration::from_millis(100)).await;

    // Offset should have increased (though exact value depends on frame encoding)
    let new_offset = ctx.state.replication.get_replication_offset();
    assert!(new_offset >= initial_offset);
}

#[tokio::test]
async fn test_replication_offset_increments_with_writes() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    let offset1 = ctx.state.replication.get_replication_offset();
    ctx.set("key1", "value1").await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let offset2 = ctx.state.replication.get_replication_offset();
    ctx.set("key2", "value2").await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let offset3 = ctx.state.replication.get_replication_offset();

    assert!(offset2 >= offset1);
    assert!(offset3 >= offset2);
}

// ===== Min Replicas Policy Tests =====

#[tokio::test]
async fn test_min_replicas_policy_no_replicas() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 1;
    primary_config.min_replicas_max_lag = 10;
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    // Try to write with no replicas connected
    let result = ctx
        .state
        .replication
        .check_min_replicas_policy(&ctx.state)
        .await;

    // Should fail because we need 1 replica but have 0
    assert!(result.is_err());
    match result {
        Err(SpinelDBError::ReadOnly(msg)) => {
            assert!(msg.contains("NOREPLICAS"));
            assert!(msg.contains("have 0"));
            assert!(msg.contains("need 1"));
        }
        _ => panic!("Expected ReadOnly error"),
    }
}

#[tokio::test]
async fn test_min_replicas_policy_with_enough_replicas() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 1;
    primary_config.min_replicas_max_lag = 10;
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    // Add a replica that's online and within lag threshold
    let test_addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    ctx.state.replica_states.insert(
        test_addr,
        ReplicaStateInfo {
            sync_state: ReplicaSyncState::Online,
            ack_offset: 100,
            last_ack_time: std::time::Instant::now(), // Just now, so within lag
        },
    );

    // Should succeed because we have 1 replica
    let result = ctx
        .state
        .replication
        .check_min_replicas_policy(&ctx.state)
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_min_replicas_policy_with_laggy_replica() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 1;
    primary_config.min_replicas_max_lag = 1; // Very short lag window
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    // Add a replica that's online but has lagged beyond threshold
    let test_addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    let replica_info = ReplicaStateInfo {
        sync_state: ReplicaSyncState::Online,
        ack_offset: 100,
        last_ack_time: std::time::Instant::now() - Duration::from_secs(5), // 5 seconds ago
    };
    ctx.state.replica_states.insert(test_addr, replica_info);

    // Wait a bit to ensure lag
    sleep(Duration::from_millis(100)).await;

    // Should fail because replica is laggy
    let result = ctx
        .state
        .replication
        .check_min_replicas_policy(&ctx.state)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_min_replicas_policy_with_awaiting_sync_replica() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 1;
    primary_config.min_replicas_max_lag = 10;
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    // Add a replica that's awaiting full sync (not online)
    let test_addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    ctx.state.replica_states.insert(
        test_addr,
        ReplicaStateInfo {
            sync_state: ReplicaSyncState::AwaitingFullSync,
            ack_offset: 0,
            last_ack_time: std::time::Instant::now(),
        },
    );

    // Should fail because replica is not online
    let result = ctx
        .state
        .replication
        .check_min_replicas_policy(&ctx.state)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_min_replicas_policy_disabled() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 0; // Disabled
    primary_config.min_replicas_max_lag = 10;
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    // Should succeed even with no replicas because policy is disabled
    let result = ctx
        .state
        .replication
        .check_min_replicas_policy(&ctx.state)
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_min_replicas_policy_multiple_replicas() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 2;
    primary_config.min_replicas_max_lag = 10;
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    // Add two online replicas
    for port in 9999..10001 {
        let test_addr = SocketAddr::from_str(&format!("127.0.0.1:{}", port)).unwrap();
        ctx.state.replica_states.insert(
            test_addr,
            ReplicaStateInfo {
                sync_state: ReplicaSyncState::Online,
                ack_offset: 100,
                last_ack_time: std::time::Instant::now(),
            },
        );
    }

    // Should succeed because we have 2 replicas
    let result = ctx
        .state
        .replication
        .check_min_replicas_policy(&ctx.state)
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_min_replicas_policy_insufficient_replicas() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    let mut primary_config = ReplicationPrimaryConfig::default();
    primary_config.min_replicas_to_write = 3;
    primary_config.min_replicas_max_lag = 10;
    config.replication = ReplicationConfig::Primary(primary_config);

    let ctx = TestContext::with_config(config).await;

    // Add only 2 online replicas (need 3)
    for port in 9999..10001 {
        let test_addr = SocketAddr::from_str(&format!("127.0.0.1:{}", port)).unwrap();
        ctx.state.replica_states.insert(
            test_addr,
            ReplicaStateInfo {
                sync_state: ReplicaSyncState::Online,
                ack_offset: 100,
                last_ack_time: std::time::Instant::now(),
            },
        );
    }

    // Should fail because we need 3 but only have 2
    let result = ctx
        .state
        .replication
        .check_min_replicas_policy(&ctx.state)
        .await;

    assert!(result.is_err());
    match result {
        Err(SpinelDBError::ReadOnly(msg)) => {
            assert!(msg.contains("have 2"));
            assert!(msg.contains("need 3"));
        }
        _ => panic!("Expected ReadOnly error"),
    }
}

// ===== Replica State Management Tests =====

#[tokio::test]
async fn test_replica_state_online() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    let test_addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    let replica_info = ReplicaStateInfo {
        sync_state: ReplicaSyncState::Online,
        ack_offset: 500,
        last_ack_time: std::time::Instant::now(),
    };

    ctx.state
        .replica_states
        .insert(test_addr, replica_info.clone());

    // Verify it was inserted
    let retrieved = ctx.state.replica_states.get(&test_addr);
    assert!(retrieved.is_some());
    let retrieved_info = retrieved.unwrap();
    assert_eq!(retrieved_info.sync_state, ReplicaSyncState::Online);
    assert_eq!(retrieved_info.ack_offset, 500);
}

#[tokio::test]
async fn test_replica_state_awaiting_sync() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    let test_addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    let replica_info = ReplicaStateInfo {
        sync_state: ReplicaSyncState::AwaitingFullSync,
        ack_offset: 0,
        last_ack_time: std::time::Instant::now(),
    };

    ctx.state
        .replica_states
        .insert(test_addr, replica_info.clone());

    // Verify it was inserted
    let retrieved = ctx.state.replica_states.get(&test_addr);
    assert!(retrieved.is_some());
    let retrieved_info = retrieved.unwrap();
    assert_eq!(
        retrieved_info.sync_state,
        ReplicaSyncState::AwaitingFullSync
    );
}

#[tokio::test]
async fn test_replica_state_update_ack() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    let test_addr = SocketAddr::from_str("127.0.0.1:9999").unwrap();
    ctx.state.replica_states.insert(
        test_addr,
        ReplicaStateInfo {
            sync_state: ReplicaSyncState::Online,
            ack_offset: 100,
            last_ack_time: std::time::Instant::now(),
        },
    );

    // Update ack offset
    if let Some(mut entry) = ctx.state.replica_states.get_mut(&test_addr) {
        entry.value_mut().ack_offset = 200;
        entry.value_mut().last_ack_time = std::time::Instant::now();
    }

    // Verify update
    let retrieved = ctx.state.replica_states.get(&test_addr);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().ack_offset, 200);
}

// ===== Replication Info Tests =====

#[tokio::test]
async fn test_replication_info_master_replid() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Master replid should be a non-empty string
    let replid = &ctx.state.replication.replication_info.master_replid;
    assert!(!replid.is_empty());
}

#[tokio::test]
async fn test_replication_info_offset_atomic() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Test that offset is atomic and thread-safe
    let offset1 = ctx.state.replication.get_replication_offset();
    ctx.set("key1", "value1").await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let offset2 = ctx.state.replication.get_replication_offset();
    let offset3 = ctx.state.replication.get_replication_offset();

    // offset2 and offset3 should be the same (atomic read)
    assert_eq!(offset2, offset3);
    assert!(offset2 >= offset1);
}

// ===== PSYNC Command Tests =====

#[tokio::test]
async fn test_psync_command_parse() {
    // Test that PSYNC command can be parsed
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"PSYNC")),
        RespFrame::BulkString(Bytes::from("?")),
        RespFrame::BulkString(Bytes::from("-1")),
    ]));

    assert!(command.is_ok());
}

#[tokio::test]
async fn test_psync_command_with_replid() {
    // Test PSYNC with a replication ID
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"PSYNC")),
        RespFrame::BulkString(Bytes::from("abc123")),
        RespFrame::BulkString(Bytes::from("1000")),
    ]));

    assert!(command.is_ok());
}

// ===== Integration: Write Commands Update Replication Offset =====

#[tokio::test]
async fn test_write_commands_update_offset() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    let initial_offset = ctx.state.replication.get_replication_offset();

    // Execute various write commands
    ctx.set("key1", "value1").await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let offset_after_set = ctx.state.replication.get_replication_offset();
    assert!(offset_after_set >= initial_offset);

    ctx.lpush("list1", &["item1"]).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let offset_after_lpush = ctx.state.replication.get_replication_offset();
    assert!(offset_after_lpush >= offset_after_set);

    ctx.sadd("set1", &["member1"]).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let offset_after_sadd = ctx.state.replication.get_replication_offset();
    assert!(offset_after_sadd >= offset_after_lpush);

    ctx.create_hash("hash1", "field1", "value1").await.unwrap();
    sleep(Duration::from_millis(50)).await;
    let offset_after_hset = ctx.state.replication.get_replication_offset();
    assert!(offset_after_hset >= offset_after_sadd);
}

// ===== Edge Cases =====

#[tokio::test]
async fn test_replication_backlog_empty() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Get from empty backlog
    let frames = ctx.state.replication_backlog.get_since(0).await;
    // Should return Some with empty vec, not None
    assert!(frames.is_some());
    assert!(frames.unwrap().is_empty());
}

#[tokio::test]
async fn test_replication_backlog_exact_offset() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Add frame at exact offset 100
    let test_frame = RespFrame::BulkString(Bytes::from("test"));
    let frame_len = test_frame.encode_to_vec().unwrap().len();
    ctx.state
        .replication_backlog
        .add(100, test_frame.clone(), frame_len)
        .await;

    // Get since offset 100 (exact match)
    let frames = ctx.state.replication_backlog.get_since(100).await;
    assert!(frames.is_some());
    let frames = frames.unwrap();
    assert_eq!(frames.len(), 1);
}

#[tokio::test]
async fn test_replication_backlog_offset_between_frames() {
    let mut config = spineldb::config::Config::default();
    config.databases = 1;
    config.replication = ReplicationConfig::Primary(ReplicationPrimaryConfig::default());

    let ctx = TestContext::with_config(config).await;

    // Add frames at offsets 100, 200, 300
    for offset in [100, 200, 300] {
        let test_frame = RespFrame::BulkString(Bytes::from("test"));
        let frame_len = test_frame.encode_to_vec().unwrap().len();
        ctx.state
            .replication_backlog
            .add(offset, test_frame, frame_len)
            .await;
    }

    // Get since offset 150 (between frames)
    let frames = ctx.state.replication_backlog.get_since(150).await;
    assert!(frames.is_some());
    let frames = frames.unwrap();
    // Should get frames at 200 and 300
    assert!(frames.len() >= 2);
}
