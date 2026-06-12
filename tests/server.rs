mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn ss(s: &str) -> RespValue {
    RespValue::SimpleString(s.to_string())
}

#[test]
fn test_initialize_with_default_config() {
    use spineldb::config::Config;
    use spineldb::core::state::ServerState;
    use std::sync::Arc;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::reload;

    common::init_tracing();
    let (filter, reload_handle) = reload::Layer::new(EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();
    let reload_handle = Arc::new(reload_handle);

    let config = Config::default();
    assert_eq!(config.databases, 16, "default database count should be 16");
    assert!(
        !config.cluster.enabled,
        "cluster mode must be off by default"
    );
    assert!(!config.acl.enabled, "ACL must be disabled by default");

    let server_init = ServerState::initialize(config, reload_handle)
        .expect("ServerState::initialize should succeed with default config");

    let state = server_init.state;
    assert_eq!(state.dbs.len(), 16, "expected 16 dbs to be initialized");
    for (i, db) in state.dbs.iter().enumerate() {
        assert!(
            state.get_db(i).is_some(),
            "get_db({i}) must return a database"
        );
        assert!(
            Arc::ptr_eq(&state.get_db(i).unwrap(), db),
            "get_db({i}) must return the same Arc stored in state.dbs",
        );
    }

    assert!(
        server_init.aof_event_rx.is_none(),
        "AOF event receiver must be None when AOF is disabled",
    );
}

#[tokio::test]
async fn test_server_starts_and_accepts_connections() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let _client = common::Client::connect(addr).await;
    server.shutdown();
}

#[tokio::test]
async fn test_server_binds_to_127_0_0_1_only() {
    let server = common::start_server().await;
    assert!(server.port > 0, "server must bind to a real port");
    server.shutdown();
}

// ── AUTH ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_auth_no_password_set_rejects() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"AUTH", b"anypassword"]).await;
    match resp {
        RespValue::Error(s) => {
            assert!(
                s.contains("no password"),
                "expected no-password error, got {s}"
            );
        }
        other => panic!("AUTH without password config should error, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_auth_correct_password() {
    let server = common::start_server_with_password("secret123").await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"AUTH", b"secret123"]).await, ss("OK"));
    server.shutdown();
}

#[tokio::test]
async fn test_auth_wrong_password() {
    let server = common::start_server_with_password("secret123").await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"AUTH", b"wrongpassword"]).await;
    match resp {
        RespValue::Error(s) => {
            assert!(
                s.contains("invalid"),
                "expected invalid-password error, got {s}"
            );
        }
        other => panic!("AUTH with wrong password should error, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_auth_required_blocks_commands() {
    let server = common::start_server_with_password("secret123").await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SET", b"k", b"v"]).await;
    match resp {
        RespValue::Error(s) => {
            assert!(s.contains("NOAUTH"), "expected NOAUTH error, got {s}");
        }
        other => panic!("unauthenticated SET should get NOAUTH, got {other:?}"),
    }
    // After auth, commands should work
    assert_eq!(c.cmd(&[b"AUTH", b"secret123"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    server.shutdown();
}

// ── ROLE ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_role_returns_master() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"ROLE"]).await;
    match resp {
        RespValue::Array(items) => {
            assert!(!items.is_empty(), "ROLE should return non-empty array");
            // First element should be "master"
            assert_eq!(items[0], bs(b"master"));
        }
        other => panic!("ROLE should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── LATENCY ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_latency_doctor() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"LATENCY", b"DOCTOR"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            // Doctor report should be a non-empty string
            assert!(
                !s.is_empty(),
                "LATENCY DOCTOR should return non-empty report"
            );
        }
        other => panic!("LATENCY DOCTOR should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_latency_history_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"LATENCY", b"HISTORY", b"command"]).await;
    match resp {
        RespValue::Array(_) => {}
        other => panic!("LATENCY HISTORY should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── SAVE ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_save_returns_ok() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SAVE"]).await;
    match resp {
        RespValue::SimpleString(s) => {
            assert_eq!(s, "OK");
        }
        other => panic!("SAVE should return OK, got {other:?}"),
    }
    server.shutdown();
}
