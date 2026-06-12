mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn ss(s: &str) -> RespValue {
    RespValue::SimpleString(s.to_string())
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

// ── EVAL ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_eval_return_string() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"EVAL", b"return 'hello'", b"0"]).await;
    assert_eq!(resp, bs(b"hello"));
    server.shutdown();
}

#[tokio::test]
async fn test_eval_return_integer() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"EVAL", b"return 42", b"0"]).await;
    assert_eq!(resp, int(42));
    server.shutdown();
}

#[tokio::test]
async fn test_eval_access_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    let resp = c
        .cmd(&[b"EVAL", b"return spinel.call('GET', KEYS[1])", b"1", b"k"])
        .await;
    assert_eq!(resp, bs(b"v"));
    server.shutdown();
}

#[tokio::test]
async fn test_eval_with_args() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"EVAL", b"return ARGV[1]", b"0", b"myarg"]).await;
    assert_eq!(resp, bs(b"myarg"));
    server.shutdown();
}

#[tokio::test]
async fn test_eval_return_table() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"EVAL", b"return {1, 'two', 3}", b"0"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], int(1));
            assert_eq!(items[1], bs(b"two"));
            assert_eq!(items[2], int(3));
        }
        other => panic!("EVAL table should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_eval_set_and_get() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[
        b"EVAL",
        b"spinel.call('SET', KEYS[1], ARGV[1])",
        b"1",
        b"k",
        b"v",
    ])
    .await;
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    server.shutdown();
}

// ── EVALSHA ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_evalsha() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let sha = match c.cmd(&[b"SCRIPT", b"LOAD", b"return 'hello'"]).await {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("SCRIPT LOAD should return BulkString, got {other:?}"),
    };

    let resp = c.cmd(&[b"EVALSHA", sha.as_bytes(), b"0"]).await;
    assert_eq!(resp, bs(b"hello"));
    server.shutdown();
}

#[tokio::test]
async fn test_evalsha_not_found() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c
        .cmd(&[
            b"EVALSHA",
            b"0000000000000000000000000000000000000000",
            b"0",
        ])
        .await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── SCRIPT EXISTS / SCRIPT FLUSH ─────────────────────────────────────────

#[tokio::test]
async fn test_script_exists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let sha = match c.cmd(&[b"SCRIPT", b"LOAD", b"return 1"]).await {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("SCRIPT LOAD should return BulkString, got {other:?}"),
    };

    let resp = c.cmd(&[b"SCRIPT", b"EXISTS", sha.as_bytes()]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], int(1));
        }
        other => panic!("SCRIPT EXISTS should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_script_flush() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"SCRIPT", b"LOAD", b"return 1"]).await;
    let resp = c.cmd(&[b"SCRIPT", b"FLUSH"]).await;
    assert_eq!(resp, ss("OK"));
    server.shutdown();
}
