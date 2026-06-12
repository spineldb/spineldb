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

// ── SET / GET basics ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_set_get_roundtrip() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    server.shutdown();
}

#[tokio::test]
async fn test_get_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"GET", b"absent"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_set_overwrites_existing_value() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"v1"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"SET", b"k", b"v2"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v2"));
    server.shutdown();
}

#[tokio::test]
async fn test_set_nx_succeeds_on_new_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"v", b"NX"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    server.shutdown();
}

#[tokio::test]
async fn test_set_nx_fails_on_existing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"v1"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"SET", b"k", b"v2", b"NX"]).await, NULL);
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v1"));
    server.shutdown();
}

#[tokio::test]
async fn test_set_xx_succeeds_on_existing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"v1"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"SET", b"k", b"v2", b"XX"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v2"));
    server.shutdown();
}

#[tokio::test]
async fn test_set_xx_fails_on_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k1", b"v1", b"XX"]).await, NULL);
    assert_eq!(c.cmd(&[b"GET", b"k1"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_set_ex_sets_ttl() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SETEX", b"k", b"100", b"v"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    let ttl = match c.cmd(&[b"TTL", b"k"]).await {
        RespValue::Integer(t) => t,
        other => panic!("TTL should be Integer, got {other:?}"),
    };
    assert!(ttl > 0 && ttl <= 100);
    server.shutdown();
}

#[tokio::test]
async fn test_psetex_sets_ttl_ms() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"PSETEX", b"k", b"100000", b"v"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    let pttl = match c.cmd(&[b"PTTL", b"k"]).await {
        RespValue::Integer(t) => t,
        other => panic!("PTTL should be Integer, got {other:?}"),
    };
    assert!(pttl > 0 && pttl <= 100000);
    server.shutdown();
}

#[tokio::test]
async fn test_set_with_get_parameter() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"old"]).await;
    let resp = c.cmd(&[b"SET", b"k", b"new", b"GET"]).await;
    assert_eq!(resp, bs(b"old"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"new"));
    server.shutdown();
}

#[tokio::test]
async fn test_set_with_get_on_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SET", b"k", b"v", b"GET"]).await;
    assert_eq!(resp, NULL);
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    server.shutdown();
}

// ── APPEND ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_append_returns_new_length() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"APPEND", b"k", b"hello"]).await, int(5));
    assert_eq!(c.cmd(&[b"APPEND", b"k", b"-world"]).await, int(11));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"hello-world"));
    server.shutdown();
}

// ── STRLEN ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_strlen_returns_byte_length() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"abcdef"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"STRLEN", b"k"]).await, int(6));
    assert_eq!(c.cmd(&[b"STRLEN", b"absent"]).await, int(0));
    server.shutdown();
}

// ── GETSET ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_getset_returns_old_and_sets_new() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"old"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GETSET", b"k", b"new"]).await, bs(b"old"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"new"));
    server.shutdown();
}

#[tokio::test]
async fn test_getset_on_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"GETSET", b"k", b"v"]).await, NULL);
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"v"));
    server.shutdown();
}

// ── GETDEL ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_getdel_returns_value_and_deletes() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GETDEL", b"k"]).await, bs(b"v"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_getdel_on_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"GETDEL", b"absent"]).await, NULL);
    server.shutdown();
}

// ── GETEX ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_getex_sets_ttl() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    let resp = c.cmd(&[b"GETEX", b"k", b"EX", b"100"]).await;
    assert_eq!(resp, bs(b"v"));
    let ttl = match c.cmd(&[b"TTL", b"k"]).await {
        RespValue::Integer(t) => t,
        other => panic!("TTL should be Integer, got {other:?}"),
    };
    assert!(ttl > 0 && ttl <= 100);
    server.shutdown();
}

#[tokio::test]
async fn test_getex_on_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"GETEX", b"absent", b"EX", b"100"]).await, NULL);
    server.shutdown();
}

// ── GETRANGE / SETRANGE ───────────────────────────────────────────────────

#[tokio::test]
async fn test_getrange_substring() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"Hello World"]).await;
    assert_eq!(c.cmd(&[b"GETRANGE", b"k", b"0", b"4"]).await, bs(b"Hello"));
    assert_eq!(c.cmd(&[b"GETRANGE", b"k", b"6", b"-1"]).await, bs(b"World"));
    assert_eq!(
        c.cmd(&[b"GETRANGE", b"k", b"0", b"-1"]).await,
        bs(b"Hello World")
    );
    server.shutdown();
}

#[tokio::test]
async fn test_getrange_on_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"GETRANGE", b"absent", b"0", b"1"]).await, bs(b""));
    server.shutdown();
}

#[tokio::test]
async fn test_setrange_replaces_bytes() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"Hello World"]).await;
    assert_eq!(c.cmd(&[b"SETRANGE", b"k", b"6", b"Redis"]).await, int(11));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"Hello Redis"));
    server.shutdown();
}

#[tokio::test]
async fn test_setrange_on_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SETRANGE", b"absent", b"0", b"Hi"]).await;
    assert_eq!(resp, int(2));
    assert_eq!(c.cmd(&[b"GET", b"absent"]).await, bs(b"Hi"));
    server.shutdown();
}

// ── INCR / DECR / INCRBY / DECRBY / INCRBYFLOAT ──────────────────────────

#[tokio::test]
async fn test_incr_decr_on_new_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"INCR", b"counter"]).await, int(1));
    assert_eq!(c.cmd(&[b"INCR", b"counter"]).await, int(2));
    assert_eq!(c.cmd(&[b"DECR", b"counter"]).await, int(1));
    assert_eq!(c.cmd(&[b"DECR", b"counter"]).await, int(0));
    assert_eq!(c.cmd(&[b"DECR", b"counter"]).await, int(-1));
    server.shutdown();
}

#[tokio::test]
async fn test_incrby_decrby() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"INCRBY", b"counter", b"10"]).await, int(10));
    assert_eq!(c.cmd(&[b"INCRBY", b"counter", b"5"]).await, int(15));
    assert_eq!(c.cmd(&[b"DECRBY", b"counter", b"7"]).await, int(8));
    server.shutdown();
}

#[tokio::test]
async fn test_incrbyfloat() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"1.5"]).await;
    let resp = c.cmd(&[b"INCRBYFLOAT", b"k", b"0.5"]).await;
    match &resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(b);
            assert!(s.starts_with("2") || s.starts_with("2.0"), "got {s}");
        }
        other => panic!("INCRBYFLOAT should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

// ── MGET / MSET / MSETNX ─────────────────────────────────────────────────

#[tokio::test]
async fn test_mget_multiple_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"a", b"1"]).await;
    c.cmd(&[b"SET", b"b", b"2"]).await;
    let resp = c.cmd(&[b"MGET", b"a", b"b", b"c"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"1"));
            assert_eq!(items[1], bs(b"2"));
            assert_eq!(items[2], NULL);
        }
        other => panic!("MGET should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_mset_sets_multiple_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3"]).await,
        ss("OK")
    );
    assert_eq!(c.cmd(&[b"GET", b"a"]).await, bs(b"1"));
    assert_eq!(c.cmd(&[b"GET", b"b"]).await, bs(b"2"));
    assert_eq!(c.cmd(&[b"GET", b"c"]).await, bs(b"3"));
    server.shutdown();
}

#[tokio::test]
async fn test_msetnx_sets_only_if_all_missing() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"existing", b"val"]).await;
    assert_eq!(
        c.cmd(&[b"MSETNX", b"existing", b"new", b"new", b"v"]).await,
        int(0)
    );
    assert_eq!(c.cmd(&[b"GET", b"existing"]).await, bs(b"val"));
    assert_eq!(c.cmd(&[b"GET", b"new"]).await, NULL);
    assert_eq!(c.cmd(&[b"MSETNX", b"x", b"1", b"y", b"2"]).await, int(1));
    assert_eq!(c.cmd(&[b"GET", b"x"]).await, bs(b"1"));
    assert_eq!(c.cmd(&[b"GET", b"y"]).await, bs(b"2"));
    server.shutdown();
}

// ── DEL / EXISTS ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_del_returns_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"a", b"1"]).await;
    c.cmd(&[b"SET", b"b", b"2"]).await;
    c.cmd(&[b"SET", b"c", b"3"]).await;
    assert_eq!(c.cmd(&[b"DEL", b"a", b"b", b"missing"]).await, int(2));
    assert_eq!(c.cmd(&[b"DEL", b"missing"]).await, int(0));
    assert_eq!(c.cmd(&[b"GET", b"c"]).await, bs(b"3"));
    server.shutdown();
}

#[tokio::test]
async fn test_exists_returns_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"a", b"1"]).await;
    c.cmd(&[b"SET", b"b", b"2"]).await;
    assert_eq!(c.cmd(&[b"EXISTS", b"a", b"b", b"missing"]).await, int(2));
    server.shutdown();
}

// ── UNLINK ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_unlink_returns_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"a", b"1"]).await;
    c.cmd(&[b"SET", b"b", b"2"]).await;
    assert_eq!(c.cmd(&[b"UNLINK", b"a", b"b", b"missing"]).await, int(2));
    assert_eq!(c.cmd(&[b"GET", b"a"]).await, NULL);
    assert_eq!(c.cmd(&[b"GET", b"b"]).await, NULL);
    server.shutdown();
}

// ── BIT commands ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_setbit_getbit() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"\x00"]).await;
    assert_eq!(c.cmd(&[b"SETBIT", b"k", b"0", b"1"]).await, int(0));
    assert_eq!(c.cmd(&[b"GETBIT", b"k", b"0"]).await, int(1));
    assert_eq!(c.cmd(&[b"GETBIT", b"k", b"1"]).await, int(0));
    server.shutdown();
}

#[tokio::test]
async fn test_bitcount() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"foobar"]).await;
    let resp = c.cmd(&[b"BITCOUNT", b"k"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n > 0, "BITCOUNT should be positive, got {n}"),
        other => panic!("BITCOUNT should be Integer, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_bitpos() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"\x00"]).await;
    c.cmd(&[b"SETBIT", b"k", b"3", b"1"]).await;
    let resp = c.cmd(&[b"BITPOS", b"k", b"1"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n >= 0, "BITPOS should be >= 0, got {n}"),
        other => panic!("BITPOS should be Integer, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_bitop_and() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"a", b"\xFF"]).await;
    c.cmd(&[b"SET", b"b", b"\x0F"]).await;
    let resp = c.cmd(&[b"BITOP", b"AND", b"dest", b"a", b"b"]).await;
    assert!(matches!(resp, RespValue::Integer(1)));
    assert_eq!(c.cmd(&[b"GET", b"dest"]).await, bs(b"\x0F"));
    server.shutdown();
}

#[tokio::test]
async fn test_bitfield() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"\x00\x00\x00\x00"]).await;
    let resp = c
        .cmd(&[b"BITFIELD", b"k", b"SET", b"u8", b"0", b"255"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], int(0));
        }
        other => panic!("BITFIELD should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── GETDEL (additional) ───────────────────────────────────────────────────

#[tokio::test]
async fn test_getdel_non_string_key_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"LPUSH", b"mylist", b"a"]).await;
    let resp = c.cmd(&[b"GETDEL", b"mylist"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── SORT on string key (WrongType) ──────────────────────────────────────

#[tokio::test]
async fn test_sort_string_key_returns_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"mystr", b"hello"]).await;
    let resp = c.cmd(&[b"SORT", b"mystr"]).await;
    assert!(
        matches!(&resp, RespValue::Error(_)),
        "SORT on string should return Error, got {resp:?}"
    );
    server.shutdown();
}

// ── RENAMENX ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_renamenx_succeeds_on_new_name() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"src", b"v"]).await;
    assert_eq!(c.cmd(&[b"RENAMENX", b"src", b"dst"]).await, int(1));
    assert_eq!(c.cmd(&[b"GET", b"src"]).await, NULL);
    assert_eq!(c.cmd(&[b"GET", b"dst"]).await, bs(b"v"));
    server.shutdown();
}

#[tokio::test]
async fn test_renamenx_fails_on_existing_name() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"src", b"v"]).await;
    c.cmd(&[b"SET", b"dst", b"x"]).await;
    assert_eq!(c.cmd(&[b"RENAMENX", b"src", b"dst"]).await, int(0));
    assert_eq!(c.cmd(&[b"GET", b"src"]).await, bs(b"v"));
    assert_eq!(c.cmd(&[b"GET", b"dst"]).await, bs(b"x"));
    server.shutdown();
}
