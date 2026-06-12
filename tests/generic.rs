mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn bs_str(s: &str) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s.as_bytes()))
}

fn ss(s: &str) -> RespValue {
    RespValue::SimpleString(s.to_string())
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

const NULL: RespValue = RespValue::Null;

// ── PING / ECHO ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ping_no_arg() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"PING"]).await, ss("PONG"));
    server.shutdown();
}

#[tokio::test]
async fn test_ping_with_arg() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"PING", b"hello"]).await, bs(b"hello"));
    server.shutdown();
}

#[tokio::test]
async fn test_echo() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"ECHO", b"round-trip"]).await, bs(b"round-trip"));
    server.shutdown();
}

// ── CLIENT ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_client_setname_getname() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"CLIENT", b"SETNAME", b"my-app"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"CLIENT", b"GETNAME"]).await, bs_str("my-app"));
    server.shutdown();
}

#[tokio::test]
async fn test_client_list_includes_self() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"CLIENT", b"LIST"]).await;
    let body = match resp {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("CLIENT LIST should return BulkString, got {other:?}"),
    };
    assert!(body.contains("addr=127.0.0.1"));
    server.shutdown();
}

// ── QUIT ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_quit_closes_connection() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"QUIT"]).await, ss("OK"));
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), c.read_frame()).await;
    let frame_result = result
        .unwrap_or_else(|_| Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")));
    assert!(frame_result.is_err());
    server.shutdown();
}

// ── SELECT / DBSIZE / FLUSHDB / FLUSHALL ─────────────────────────────────

#[tokio::test]
async fn test_select_switches_db() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"in-db-0"]).await;
    assert_eq!(c.cmd(&[b"SELECT", b"1"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, NULL);
    assert_eq!(c.cmd(&[b"SELECT", b"0"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, bs(b"in-db-0"));
    server.shutdown();
}

#[tokio::test]
async fn test_select_out_of_range_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SELECT", b"99"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_dbsize_empty_and_after_sets() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"DBSIZE"]).await, int(0));
    c.cmd(&[b"SET", b"a", b"1"]).await;
    c.cmd(&[b"SET", b"b", b"2"]).await;
    assert_eq!(c.cmd(&[b"DBSIZE"]).await, int(2));
    server.shutdown();
}

#[tokio::test]
async fn test_flushdb_clears_current_db_only() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k0", b"v"]).await;
    c.cmd(&[b"SELECT", b"1"]).await;
    c.cmd(&[b"SET", b"k1", b"v"]).await;
    assert_eq!(c.cmd(&[b"FLUSHDB"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"DBSIZE"]).await, int(0));
    assert_eq!(c.cmd(&[b"SELECT", b"0"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"DBSIZE"]).await, int(1));
    server.shutdown();
}

#[tokio::test]
async fn test_flushall_clears_all_databases() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k1", b"v1"]).await;
    c.cmd(&[b"SELECT", b"1"]).await;
    c.cmd(&[b"SET", b"k2", b"v2"]).await;
    assert_eq!(c.cmd(&[b"FLUSHALL"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"SELECT", b"0"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k1"]).await, NULL);
    assert_eq!(c.cmd(&[b"SELECT", b"1"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"k2"]).await, NULL);
    server.shutdown();
}

// ── KEYS / TYPE / RENAME / RENAMENX ──────────────────────────────────────

#[tokio::test]
async fn test_keys_returns_matching() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"user:1", b"a"]).await;
    c.cmd(&[b"SET", b"user:2", b"b"]).await;
    c.cmd(&[b"SET", b"other", b"c"]).await;
    let resp = c.cmd(&[b"KEYS", b"user:*"]).await;
    let mut keys: Vec<String> = match resp {
        RespValue::Array(items) => items
            .into_iter()
            .map(|v| match v {
                RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
                other => panic!("KEYS array should contain BulkStrings, got {other:?}"),
            })
            .collect(),
        other => panic!("KEYS should return Array, got {other:?}"),
    };
    keys.sort();
    assert_eq!(keys, vec!["user:1", "user:2"]);
    server.shutdown();
}

#[tokio::test]
async fn test_type_returns_correct_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"SET", b"str", b"hello"]).await;
    assert_eq!(c.cmd(&[b"TYPE", b"str"]).await, ss("string"));

    c.cmd(&[b"LPUSH", b"lst", b"item1"]).await;
    assert_eq!(c.cmd(&[b"TYPE", b"lst"]).await, ss("list"));

    c.cmd(&[b"SADD", b"st", b"member1"]).await;
    assert_eq!(c.cmd(&[b"TYPE", b"st"]).await, ss("set"));

    c.cmd(&[b"ZADD", b"zst", b"1.0", b"member1"]).await;
    assert_eq!(c.cmd(&[b"TYPE", b"zst"]).await, ss("zset"));

    c.cmd(&[b"HSET", b"hsh", b"f1", b"v1"]).await;
    assert_eq!(c.cmd(&[b"TYPE", b"hsh"]).await, ss("hash"));

    assert_eq!(c.cmd(&[b"TYPE", b"nonexistent"]).await, ss("none"));
    server.shutdown();
}

#[tokio::test]
async fn test_rename_moves_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"src", b"v"]).await;
    assert_eq!(c.cmd(&[b"RENAME", b"src", b"dst"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"GET", b"src"]).await, NULL);
    assert_eq!(c.cmd(&[b"GET", b"dst"]).await, bs(b"v"));
    server.shutdown();
}

#[tokio::test]
async fn test_rename_missing_source_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"RENAME", b"absent", b"dst"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

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

// ── TTL / PTTL / EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT / PERSIST ──────

#[tokio::test]
async fn test_expire_and_ttl() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    assert_eq!(c.cmd(&[b"EXPIRE", b"k", b"100"]).await, int(1));
    let ttl = match c.cmd(&[b"TTL", b"k"]).await {
        RespValue::Integer(t) => t,
        other => panic!("TTL should be Integer, got {other:?}"),
    };
    assert!(ttl > 0 && ttl <= 100);
    server.shutdown();
}

#[tokio::test]
async fn test_ttl_missing_key_returns_minus_two() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"TTL", b"absent"]).await, int(-2));
    server.shutdown();
}

#[tokio::test]
async fn test_ttl_key_without_expiry_returns_minus_one() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    assert_eq!(c.cmd(&[b"TTL", b"k"]).await, int(-1));
    server.shutdown();
}

#[tokio::test]
async fn test_persist_removes_expiry() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    c.cmd(&[b"EXPIRE", b"k", b"100"]).await;
    assert_eq!(c.cmd(&[b"PERSIST", b"k"]).await, int(1));
    assert_eq!(c.cmd(&[b"TTL", b"k"]).await, int(-1));
    server.shutdown();
}

#[tokio::test]
async fn test_pexpire_sets_ttl_ms() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    assert_eq!(c.cmd(&[b"PEXPIRE", b"k", b"100000"]).await, int(1));
    let pttl = match c.cmd(&[b"PTTL", b"k"]).await {
        RespValue::Integer(t) => t,
        other => panic!("PTTL should be Integer, got {other:?}"),
    };
    assert!(pttl > 0 && pttl <= 100000);
    server.shutdown();
}

#[tokio::test]
async fn test_expireat_sets_timestamp() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let resp = c
        .cmd(&[b"EXPIREAT", b"k", future.to_string().as_bytes()])
        .await;
    assert_eq!(resp, int(1));
    let ttl = match c.cmd(&[b"TTL", b"k"]).await {
        RespValue::Integer(t) => t,
        other => panic!("TTL should be Integer, got {other:?}"),
    };
    assert!(ttl > 0 && ttl <= 3600);
    server.shutdown();
}

// ── TIME ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_time() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"TIME"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            // Server may return either Integer or BulkString for time values
            for item in &items {
                match item {
                    RespValue::Integer(_) | RespValue::BulkString(_) => {}
                    other => panic!("TIME element should be Integer or BulkString, got {other:?}"),
                }
            }
        }
        other => panic!("TIME should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── MEMORY USAGE ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_memory_usage() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    let resp = c.cmd(&[b"MEMORY", b"USAGE", b"k"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(n > 0, "MEMORY USAGE should be positive, got {n}");
        }
        other => panic!("MEMORY USAGE should return Integer, got {other:?}"),
    }
    server.shutdown();
}

// ── SLOWLOG ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_slowlog_len() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SLOWLOG", b"LEN"]).await;
    match resp {
        RespValue::Integer(_) => {}
        other => panic!("SLOWLOG LEN should return Integer, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_slowlog_get() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SLOWLOG", b"GET", b"10"]).await;
    match resp {
        RespValue::Array(_) => {}
        other => panic!("SLOWLOG GET should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── LASTSAVE ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_lastsave() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"LASTSAVE"]).await;
    match resp {
        RespValue::Integer(_) => {}
        other => panic!("LASTSAVE should return Integer, got {other:?}"),
    }
    server.shutdown();
}

// ── COMMAND (info) ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_command_info() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"COMMAND"]).await;
    match resp {
        RespValue::Array(_) => {}
        RespValue::Error(_) => {}
        other => panic!("COMMAND should return Array or Error, got {other:?}"),
    }
    server.shutdown();
}

// ── SCAN (cursor pagination) ─────────────────────────────────────────────

#[tokio::test]
async fn test_scan_pagination_returns_all_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    for i in 0..50 {
        let key = format!("scankey:{:03}", i);
        let val = format!("val{}", i);
        c.cmd(&[b"SET", key.as_bytes(), val.as_bytes()]).await;
    }

    let mut all_keys = std::collections::HashSet::new();
    let mut cursor: i64 = 0;
    loop {
        let cursor_str = cursor.to_string();
        let resp = c
            .cmd(&[b"SCAN", cursor_str.as_bytes(), b"COUNT", b"10"])
            .await;
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                cursor = match &arr[0] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap(),
                    RespValue::Integer(i) => *i,
                    other => panic!("SCAN cursor should be integer, got {:?}", other),
                };
                if let RespValue::Array(keys) = &arr[1] {
                    for key in keys {
                        if let RespValue::BulkString(b) = key {
                            all_keys.insert(String::from_utf8_lossy(b).into_owned());
                        }
                    }
                }
            }
            other => panic!("SCAN should return Array, got {:?}", other),
        }
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(all_keys.len(), 50);
    server.shutdown();
}

#[tokio::test]
async fn test_scan_empty_db_returns_cursor_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SCAN", b"0"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[0] {
                RespValue::BulkString(b) => {
                    assert_eq!(String::from_utf8_lossy(b).as_ref(), "0");
                }
                RespValue::Integer(i) => assert_eq!(*i, 0),
                other => panic!("expected cursor, got {:?}", other),
            }
            match &arr[1] {
                RespValue::Array(keys) => assert!(keys.is_empty()),
                other => panic!("expected empty key array, got {:?}", other),
            }
        }
        other => panic!("SCAN should return Array, got {:?}", other),
    }
    server.shutdown();
}

// ── HSCAN / SSCAN / ZSCAN ────────────────────────────────────────────────

#[tokio::test]
async fn test_hscan_returns_all_fields() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"])
        .await;
    let mut all = std::collections::HashMap::new();
    let mut cursor: i64 = 0;
    loop {
        let cursor_str = cursor.to_string();
        let resp = c.cmd(&[b"HSCAN", b"h", cursor_str.as_bytes()]).await;
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                cursor = match &arr[0] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap(),
                    other => panic!("cursor should be BulkString, got {other:?}"),
                };
                if let RespValue::Array(kvs) = &arr[1] {
                    let mut iter = kvs.iter();
                    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                        if let (RespValue::BulkString(kb), RespValue::BulkString(vb)) = (k, v) {
                            all.insert(
                                String::from_utf8_lossy(kb).into_owned(),
                                String::from_utf8_lossy(vb).into_owned(),
                            );
                        }
                    }
                }
            }
            other => panic!("HSCAN should return Array, got {other:?}"),
        }
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(all.len(), 3);
    assert_eq!(all.get("f1").unwrap(), "v1");
    assert_eq!(all.get("f2").unwrap(), "v2");
    assert_eq!(all.get("f3").unwrap(), "v3");
    server.shutdown();
}

#[tokio::test]
async fn test_hscan_with_match_pattern() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2", b"o1", b"x"])
        .await;
    let mut all = std::collections::HashSet::new();
    let mut cursor: i64 = 0;
    loop {
        let cursor_str = cursor.to_string();
        let resp = c
            .cmd(&[b"HSCAN", b"h", cursor_str.as_bytes(), b"MATCH", b"f*"])
            .await;
        match resp {
            RespValue::Array(arr) => {
                cursor = match &arr[0] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap(),
                    other => panic!("cursor should be BulkString, got {other:?}"),
                };
                if let RespValue::Array(kvs) = &arr[1] {
                    for i in (0..kvs.len()).step_by(2) {
                        if let RespValue::BulkString(kb) = &kvs[i] {
                            all.insert(String::from_utf8_lossy(kb).into_owned());
                        }
                    }
                }
            }
            other => panic!("HSCAN should return Array, got {other:?}"),
        }
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(all.len(), 2);
    assert!(all.contains("f1"));
    assert!(all.contains("f2"));
    assert!(!all.contains("o1"));
    server.shutdown();
}

#[tokio::test]
async fn test_hscan_missing_key_returns_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"HSCAN", b"nonexistent", b"0"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[0] {
                RespValue::BulkString(b) => {
                    assert_eq!(String::from_utf8_lossy(b).as_ref(), "0");
                }
                other => panic!("cursor should be BulkString, got {other:?}"),
            }
            if let RespValue::Array(keys) = &arr[1] {
                assert!(keys.is_empty());
            }
        }
        other => panic!("HSCAN should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_sscan_returns_all_members() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SADD", b"s", b"a", b"b", b"c"]).await;
    let mut all = std::collections::HashSet::new();
    let mut cursor: i64 = 0;
    loop {
        let cursor_str = cursor.to_string();
        let resp = c.cmd(&[b"SSCAN", b"s", cursor_str.as_bytes()]).await;
        match resp {
            RespValue::Array(arr) => {
                cursor = match &arr[0] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap(),
                    other => panic!("cursor should be BulkString, got {other:?}"),
                };
                if let RespValue::Array(members) = &arr[1] {
                    for m in members {
                        if let RespValue::BulkString(mb) = m {
                            all.insert(String::from_utf8_lossy(mb).into_owned());
                        }
                    }
                }
            }
            other => panic!("SSCAN should return Array, got {other:?}"),
        }
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(all.len(), 3);
    assert!(all.contains("a"));
    assert!(all.contains("b"));
    assert!(all.contains("c"));
    server.shutdown();
}

#[tokio::test]
async fn test_sscan_missing_key_returns_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SSCAN", b"nonexistent", b"0"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[0] {
                RespValue::BulkString(b) => {
                    assert_eq!(String::from_utf8_lossy(b).as_ref(), "0");
                }
                other => panic!("cursor should be BulkString, got {other:?}"),
            }
            if let RespValue::Array(members) = &arr[1] {
                assert!(members.is_empty());
            }
        }
        other => panic!("SSCAN should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_zscan_returns_all_members() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"zs", b"1.0", b"a", b"2.0", b"b", b"3.0", b"c"])
        .await;
    let mut all = std::collections::HashMap::new();
    let mut cursor: i64 = 0;
    loop {
        let cursor_str = cursor.to_string();
        let resp = c.cmd(&[b"ZSCAN", b"zs", cursor_str.as_bytes()]).await;
        match resp {
            RespValue::Array(arr) => {
                cursor = match &arr[0] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap(),
                    other => panic!("cursor should be BulkString, got {other:?}"),
                };
                if let RespValue::Array(items) = &arr[1] {
                    let mut iter = items.iter();
                    while let (Some(member), Some(score)) = (iter.next(), iter.next()) {
                        if let (RespValue::BulkString(mb), RespValue::BulkString(sb)) =
                            (member, score)
                        {
                            all.insert(
                                String::from_utf8_lossy(mb).into_owned(),
                                String::from_utf8_lossy(sb).into_owned(),
                            );
                        }
                    }
                }
            }
            other => panic!("ZSCAN should return Array, got {other:?}"),
        }
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(all.len(), 3);
    assert_eq!(all.get("a").unwrap(), "1");
    assert_eq!(all.get("b").unwrap(), "2");
    assert_eq!(all.get("c").unwrap(), "3");
    server.shutdown();
}

#[tokio::test]
async fn test_zscan_missing_key_returns_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"ZSCAN", b"nonexistent", b"0"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[0] {
                RespValue::BulkString(b) => {
                    assert_eq!(String::from_utf8_lossy(b).as_ref(), "0");
                }
                other => panic!("cursor should be BulkString, got {other:?}"),
            }
            if let RespValue::Array(members) = &arr[1] {
                assert!(members.is_empty());
            }
        }
        other => panic!("ZSCAN should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── SORT ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_sort_list_numeric() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"3", b"1", b"2"]).await;
    let resp = c.cmd(&[b"SORT", b"mylist"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"1"), bs(b"2"), bs(b"3")]);
        }
        other => panic!("SORT should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_sort_list_alpha() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"banana", b"apple", b"cherry"])
        .await;
    let resp = c.cmd(&[b"SORT", b"mylist", b"ALPHA"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"apple"), bs(b"banana"), bs(b"cherry")]);
        }
        other => panic!("SORT ALPHA should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_sort_list_desc() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"RPUSH", b"mylist", b"3", b"1", b"2"]).await;
    let resp = c.cmd(&[b"SORT", b"mylist", b"DESC"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"3"), bs(b"2"), bs(b"1")]);
        }
        other => panic!("SORT DESC should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_sort_set() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SADD", b"myset", b"5", b"1", b"3"]).await;
    let resp = c.cmd(&[b"SORT", b"myset"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"1"), bs(b"3"), bs(b"5")]);
        }
        other => panic!("SORT on set should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_sort_missing_key_returns_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SORT", b"nosuchkey"]).await;
    assert_eq!(resp, RespValue::Array(vec![]));
    server.shutdown();
}

// ── WATCH / UNWATCH ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_watch_unwatch() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    assert_eq!(c.cmd(&[b"WATCH", b"k"]).await, ss("OK"));
    assert_eq!(c.cmd(&[b"UNWATCH"]).await, ss("OK"));
    server.shutdown();
}

// ── BGSAVE / BGREWRITEAOF (just check no error) ─────────────────────────

#[tokio::test]
async fn test_bgsave() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BGSAVE"]).await;
    match resp {
        RespValue::SimpleString(_) => {}
        RespValue::Error(_) => {}
        other => panic!("BGSAVE should return SimpleString or Error, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_bgrerewriteaof() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BGREWRITEAOF"]).await;
    match resp {
        RespValue::SimpleString(_) => {}
        RespValue::Error(_) => {}
        other => panic!("BGREWRITEAOF should return SimpleString or Error, got {other:?}"),
    }
    server.shutdown();
}

// ── Error handling ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_unknown_command_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"DEFINITELY_NOT_A_REAL_COMMAND"]).await;
    match resp {
        RespValue::Error(s) => {
            assert!(s.contains("Unknown command"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_wrong_arity_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"SET", b"only-key"]).await;
    match resp {
        RespValue::Error(s) => {
            assert!(s.to_lowercase().contains("wrong number of arguments"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_incr_on_non_integer_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"not-a-number"]).await;
    let resp = c.cmd(&[b"INCR", b"k"]).await;
    match resp {
        RespValue::Error(s) => {
            assert!(s.to_lowercase().contains("not an integer"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
    server.shutdown();
}

// ── PTTL ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_pttl_missing_key_returns_minus_two() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"PTTL", b"absent"]).await, int(-2));
    server.shutdown();
}

#[tokio::test]
async fn test_pttl_key_without_expiry_returns_minus_one() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    assert_eq!(c.cmd(&[b"PTTL", b"k"]).await, int(-1));
    server.shutdown();
}

#[tokio::test]
async fn test_pttl_returns_milliseconds() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    c.cmd(&[b"PEXPIRE", b"k", b"50000"]).await;
    let resp = c.cmd(&[b"PTTL", b"k"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(n > 0 && n <= 50000, "PTTL should be in (0, 50000], got {n}");
        }
        other => panic!("PTTL should return Integer, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_pttl_after_expire_returns_milliseconds() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    c.cmd(&[b"EXPIRE", b"k", b"100"]).await;
    let resp = c.cmd(&[b"PTTL", b"k"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(
                n > 0 && n <= 100000,
                "PTTL after EXPIRE should be in (0, 100000], got {n}"
            );
        }
        other => panic!("PTTL should return Integer, got {other:?}"),
    }
    server.shutdown();
}

// ── PEXPIREAT ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_pexpireat_sets_expiry() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 3600000; // 1 hour from now
    let resp = c
        .cmd(&[b"PEXPIREAT", b"k", future_ms.to_string().as_bytes()])
        .await;
    assert_eq!(resp, int(1));
    let pttl = match c.cmd(&[b"PTTL", b"k"]).await {
        RespValue::Integer(t) => t,
        other => panic!("PTTL should be Integer, got {other:?}"),
    };
    assert!(pttl > 0 && pttl <= 3600000);
    server.shutdown();
}

#[tokio::test]
async fn test_pexpireat_missing_key_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 3600000;
    assert_eq!(
        c.cmd(&[b"PEXPIREAT", b"absent", future_ms.to_string().as_bytes(),])
            .await,
        int(0)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_pexpireat_past_time_expires_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"v"]).await;
    let past_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 1000; // 1 second in the past
    let resp = c
        .cmd(&[b"PEXPIREAT", b"k", past_ms.to_string().as_bytes()])
        .await;
    assert_eq!(resp, int(1));
    // Key should be expired now
    assert_eq!(c.cmd(&[b"GET", b"k"]).await, NULL);
    server.shutdown();
}
