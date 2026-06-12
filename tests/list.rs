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

const NULL: RespValue = RespValue::Null;

// ── LPUSH / RPUSH / LPOP / RPOP ──────────────────────────────────────────

#[tokio::test]
async fn test_list_lpush_rpush_lpop_rpop() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"LPUSH", b"mylist", b"a"]).await, int(1));
    assert_eq!(c.cmd(&[b"LPUSH", b"mylist", b"b"]).await, int(2));
    assert_eq!(c.cmd(&[b"RPUSH", b"mylist", b"c"]).await, int(3));
    assert_eq!(c.cmd(&[b"LPOP", b"mylist"]).await, bs(b"b"));
    assert_eq!(c.cmd(&[b"RPOP", b"mylist"]).await, bs(b"c"));
    assert_eq!(c.cmd(&[b"LPOP", b"mylist"]).await, bs(b"a"));
    assert_eq!(c.cmd(&[b"LPOP", b"mylist"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_list_lpop_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"c"]).await;
    let resp = c.cmd(&[b"LPOP", b"mylist", b"2"]).await;
    assert!(
        matches!(&resp, RespValue::Error(_)),
        "LPOP count not supported, got {resp:?}"
    );
    assert_eq!(c.cmd(&[b"LLEN", b"mylist"]).await, int(3));
    server.shutdown();
}

#[tokio::test]
async fn test_list_rpop_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"c"]).await;
    let resp = c.cmd(&[b"RPOP", b"mylist", b"2"]).await;
    assert!(
        matches!(&resp, RespValue::Error(_)),
        "RPOP count not supported, got {resp:?}"
    );
    assert_eq!(c.cmd(&[b"LLEN", b"mylist"]).await, int(3));
    server.shutdown();
}

// ── LPUSHX / RPUSHX ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_lpushx_rpushx() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"LPUSHX", b"mylist", b"a"]).await, int(0));
    assert_eq!(c.cmd(&[b"RPUSHX", b"mylist", b"b"]).await, int(0));
    c.cmd(&[b"RPUSH", b"mylist", b"c"]).await;
    assert_eq!(c.cmd(&[b"LPUSHX", b"mylist", b"d"]).await, int(2));
    assert_eq!(c.cmd(&[b"RPUSHX", b"mylist", b"e"]).await, int(3));
    server.shutdown();
}

// ── LLEN ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_llen() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"LLEN", b"mylist"]).await, int(0));
    c.cmd(&[b"RPUSH", b"mylist", b"x", b"y"]).await;
    assert_eq!(c.cmd(&[b"LLEN", b"mylist"]).await, int(2));
    server.shutdown();
}

// ── LRANGE ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_lrange() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"c", b"d", b"e"])
        .await;
    let resp = c.cmd(&[b"LRANGE", b"mylist", b"1", b"3"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"b"), bs(b"c"), bs(b"d")]);
        }
        other => panic!("LRANGE should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_list_lrange_out_of_range() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a"]).await;
    let resp = c.cmd(&[b"LRANGE", b"mylist", b"0", b"100"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"a")]);
        }
        other => panic!("LRANGE out of range should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_list_lrange_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"LRANGE", b"absent", b"0", b"-1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert!(items.is_empty());
        }
        other => panic!("LRANGE empty should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── LINDEX ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_lindex() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"c"]).await;
    assert_eq!(c.cmd(&[b"LINDEX", b"mylist", b"0"]).await, bs(b"a"));
    assert_eq!(c.cmd(&[b"LINDEX", b"mylist", b"1"]).await, bs(b"b"));
    assert_eq!(c.cmd(&[b"LINDEX", b"mylist", b"-1"]).await, bs(b"c"));
    assert_eq!(c.cmd(&[b"LINDEX", b"mylist", b"10"]).await, NULL);
    server.shutdown();
}

// ── LSET ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_lset() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"c"]).await;
    assert_eq!(c.cmd(&[b"LSET", b"mylist", b"1", b"x"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"LINDEX", b"mylist", b"1"]).await, bs(b"x"));
    server.shutdown();
}

#[tokio::test]
async fn test_list_lset_out_of_range() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a"]).await;
    let resp = c.cmd(&[b"LSET", b"mylist", b"10", b"x"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── LREM ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_lrem() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"a", b"c", b"a"])
        .await;
    assert_eq!(c.cmd(&[b"LREM", b"mylist", b"2", b"a"]).await, int(2));
    assert_eq!(c.cmd(&[b"LLEN", b"mylist"]).await, int(3));
    server.shutdown();
}

// ── LTRIM ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_ltrim() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"c", b"d", b"e"])
        .await;
    assert_eq!(c.cmd(&[b"LTRIM", b"mylist", b"1", b"3"]).await, ss("OK"));
    let resp = c.cmd(&[b"LRANGE", b"mylist", b"0", b"-1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"b"), bs(b"c"), bs(b"d")]);
        }
        other => panic!("LRANGE after LTRIM should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── LINSERT ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_linsert_before() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"c"]).await;
    assert_eq!(
        c.cmd(&[b"LINSERT", b"mylist", b"BEFORE", b"c", b"b"]).await,
        int(3)
    );
    let resp = c.cmd(&[b"LRANGE", b"mylist", b"0", b"-1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"a"), bs(b"b"), bs(b"c")]);
        }
        other => panic!("LRANGE should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_list_linsert_after() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b"]).await;
    assert_eq!(
        c.cmd(&[b"LINSERT", b"mylist", b"AFTER", b"b", b"c"]).await,
        int(3)
    );
    let resp = c.cmd(&[b"LRANGE", b"mylist", b"0", b"-1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"a"), bs(b"b"), bs(b"c")]);
        }
        other => panic!("LRANGE should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── LMOVE ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_lmove() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"src", b"a", b"b"]).await;
    c.cmd(&[b"RPUSH", b"dst", b"x"]).await;
    assert_eq!(
        c.cmd(&[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]).await,
        bs(b"a")
    );
    assert_eq!(
        c.cmd(&[b"LRANGE", b"src", b"0", b"-1"]).await,
        RespValue::Array(vec![bs(b"b")])
    );
    assert_eq!(
        c.cmd(&[b"LRANGE", b"dst", b"0", b"-1"]).await,
        RespValue::Array(vec![bs(b"x"), bs(b"a")])
    );
    server.shutdown();
}

// ── LPOS ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_lpos() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"c"]).await;
    assert_eq!(c.cmd(&[b"LPOS", b"mylist", b"a"]).await, int(0));
    assert_eq!(c.cmd(&[b"LPOS", b"mylist", b"c"]).await, int(2));
    assert_eq!(c.cmd(&[b"LPOS", b"mylist", b"z"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_list_lpos_rank() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b", b"a"]).await;
    assert_eq!(
        c.cmd(&[b"LPOS", b"mylist", b"a", b"RANK", b"2"]).await,
        int(2)
    );
    server.shutdown();
}

// ── BLPOP / BRPOP (blocking - use timeout) ────────────────────────────────

#[tokio::test]
async fn test_blpop_with_data() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a"]).await;
    let resp = c.cmd(&[b"BLPOP", b"mylist", b"1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"mylist"), bs(b"a")]);
        }
        other => panic!("BLPOP should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_blpop_timeout() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BLPOP", b"empty", b"1"]).await;
    assert_eq!(resp, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_brpop_with_data() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"a", b"b"]).await;
    let resp = c.cmd(&[b"BRPOP", b"mylist", b"1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"mylist"), bs(b"b")]);
        }
        other => panic!("BRPOP should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── BLMOVE ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_blmove_with_data() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"src", b"a", b"b", b"c"]).await;
    let resp = c
        .cmd(&[b"BLMOVE", b"src", b"dst", b"LEFT", b"RIGHT", b"1"])
        .await;
    match resp {
        RespValue::BulkString(b) => {
            assert_eq!(String::from_utf8_lossy(&b).as_ref(), "a");
        }
        other => panic!("BLMOVE should return BulkString, got {other:?}"),
    }
    assert_eq!(c.cmd(&[b"LLEN", b"src"]).await, RespValue::Integer(2));
    assert_eq!(c.cmd(&[b"LLEN", b"dst"]).await, RespValue::Integer(1));
    server.shutdown();
}

#[tokio::test]
async fn test_blmove_timeout() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c
        .cmd(&[b"BLMOVE", b"empty", b"dst", b"LEFT", b"RIGHT", b"1"])
        .await;
    assert_eq!(resp, NULL);
    server.shutdown();
}
