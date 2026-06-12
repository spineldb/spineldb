mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

const NULL: RespValue = RespValue::Null;

// ── HSET / HGET ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_set_get() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"HSET", b"myhash", b"f1", b"v1"]).await, int(1));
    assert_eq!(c.cmd(&[b"HGET", b"myhash", b"f1"]).await, bs(b"v1"));
    assert_eq!(c.cmd(&[b"HGET", b"myhash", b"missing"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_hash_set_multiple() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"HSET", b"myhash", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"])
            .await,
        int(3)
    );
    assert_eq!(c.cmd(&[b"HGET", b"myhash", b"f2"]).await, bs(b"v2"));
    server.shutdown();
}

// ── HSETNX ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_setnx() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"HSETNX", b"myhash", b"f1", b"v1"]).await, int(1));
    assert_eq!(c.cmd(&[b"HSETNX", b"myhash", b"f1", b"v2"]).await, int(0));
    assert_eq!(c.cmd(&[b"HGET", b"myhash", b"f1"]).await, bs(b"v1"));
    server.shutdown();
}

// ── HGETALL ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hgetall() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1"]).await;
    c.cmd(&[b"HSET", b"myhash", b"f2", b"v2"]).await;
    let resp = c.cmd(&[b"HGETALL", b"myhash"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], bs(b"f1"));
            assert_eq!(items[1], bs(b"v1"));
            assert_eq!(items[2], bs(b"f2"));
            assert_eq!(items[3], bs(b"v2"));
        }
        _ => panic!("HGETALL should return Array"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_hash_hgetall_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"HGETALL", b"empty"]).await;
    match resp {
        RespValue::Array(items) => assert!(items.is_empty()),
        _ => panic!("HGETALL empty should return Array"),
    }
    server.shutdown();
}

// ── HDEL / HLEN ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hdel_hlen() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1", b"f2", b"v2"])
        .await;
    assert_eq!(c.cmd(&[b"HLEN", b"myhash"]).await, int(2));
    assert_eq!(c.cmd(&[b"HDEL", b"myhash", b"f1"]).await, int(1));
    assert_eq!(c.cmd(&[b"HLEN", b"myhash"]).await, int(1));
    assert_eq!(c.cmd(&[b"HDEL", b"myhash", b"missing"]).await, int(0));
    server.shutdown();
}

// ── HEXISTS ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hexists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1"]).await;
    assert_eq!(c.cmd(&[b"HEXISTS", b"myhash", b"f1"]).await, int(1));
    assert_eq!(c.cmd(&[b"HEXISTS", b"myhash", b"missing"]).await, int(0));
    server.shutdown();
}

// ── HKEYS / HVALS ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hkeys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1", b"f2", b"v2"])
        .await;
    let resp = c.cmd(&[b"HKEYS", b"myhash"]).await;
    match resp {
        RespValue::Array(mut items) => {
            items.sort_by(|a, b| {
                let a = match a {
                    RespValue::BulkString(b) => b.clone(),
                    _ => panic!(),
                };
                let b = match b {
                    RespValue::BulkString(b) => b.clone(),
                    _ => panic!(),
                };
                a.cmp(&b)
            });
            assert_eq!(items, vec![bs(b"f1"), bs(b"f2")]);
        }
        other => panic!("HKEYS should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_hash_hvals() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1", b"f2", b"v2"])
        .await;
    let resp = c.cmd(&[b"HVALS", b"myhash"]).await;
    match resp {
        RespValue::Array(mut items) => {
            items.sort_by(|a, b| {
                let a = match a {
                    RespValue::BulkString(b) => b.clone(),
                    _ => panic!(),
                };
                let b = match b {
                    RespValue::BulkString(b) => b.clone(),
                    _ => panic!(),
                };
                a.cmp(&b)
            });
            assert_eq!(items, vec![bs(b"v1"), bs(b"v2")]);
        }
        other => panic!("HVALS should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── HMGET ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hmget() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1", b"f2", b"v2"])
        .await;
    let resp = c
        .cmd(&[b"HMGET", b"myhash", b"f1", b"missing", b"f2"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"v1"), NULL, bs(b"v2")]);
        }
        other => panic!("HMGET should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── HINCRBY / HINCRBYFLOAT ────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hincrby() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"counter", b"10"]).await;
    assert_eq!(
        c.cmd(&[b"HINCRBY", b"myhash", b"counter", b"5"]).await,
        int(15)
    );
    assert_eq!(
        c.cmd(&[b"HINCRBY", b"myhash", b"counter", b"-3"]).await,
        int(12)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_hash_hincrbyfloat() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"score", b"1.5"]).await;
    let resp = c.cmd(&[b"HINCRBYFLOAT", b"myhash", b"score", b"0.5"]).await;
    match &resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(b);
            assert!(s.starts_with("2"), "got {s}");
        }
        other => panic!("HINCRBYFLOAT should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

// ── HRANDFIELD ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hrandfield() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"])
        .await;
    let resp = c.cmd(&[b"HRANDFIELD", b"myhash"]).await;
    match &resp {
        RespValue::BulkString(_) => {}
        other => panic!("HRANDFIELD should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_hash_hrandfield_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"])
        .await;
    let resp = c.cmd(&[b"HRANDFIELD", b"myhash", b"2"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        other => panic!("HRANDFIELD count should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── HSTRLEN ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hstrlen() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"myhash", b"f1", b"hello"]).await;
    assert_eq!(c.cmd(&[b"HSTRLEN", b"myhash", b"f1"]).await, int(5));
    assert_eq!(c.cmd(&[b"HSTRLEN", b"myhash", b"missing"]).await, int(0));
    server.shutdown();
}
