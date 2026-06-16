mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn ss(s: &str) -> RespValue {
    RespValue::SimpleString(s.to_string())
}

#[tokio::test]
async fn test_json_set_get() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(
        client
            .cmd(&[
                b"JSON.SET",
                b"user:1",
                b"$",
                b"{\"name\":\"Alice\",\"age\":30}"
            ])
            .await,
        ss("OK")
    );
    let resp = client.cmd(&[b"JSON.GET", b"user:1"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("name"));
            assert!(s.contains("Alice"));
        }
        _ => panic!("JSON.GET should return BulkString"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_numincrby() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    client.cmd(&[b"JSON.SET", b"counter", b"$", b"10"]).await;
    assert_eq!(
        client
            .cmd(&[b"JSON.NUMINCRBY", b"counter", b"$", b"5"])
            .await,
        bs(b"15")
    );
    assert_eq!(
        client
            .cmd(&[b"JSON.NUMINCRBY", b"counter", b"$", b"-3"])
            .await,
        bs(b"12")
    );
    server.shutdown();
}

// ── JSON.DEL ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_del_existing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1,\"b\":2}"])
        .await;
    assert_eq!(
        c.cmd(&[b"JSON.DEL", b"k", b"$.a"]).await,
        RespValue::Integer(1)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(!s.contains("a"));
            assert!(s.contains("b"));
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_del_missing_key_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.DEL", b"nonexistent", b"$.a"]).await,
        RespValue::Integer(0)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_del_root_path() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    assert_eq!(c.cmd(&[b"JSON.DEL", b"k"]).await, RespValue::Integer(1));
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s.as_ref(), "null");
        }
        other => panic!("JSON.GET should return BulkString (null), got {other:?}"),
    }
    server.shutdown();
}

// ── JSON.TYPE ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_type_object() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    assert_eq!(c.cmd(&[b"JSON.TYPE", b"k"]).await, ss("object"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_type_array() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3]"]).await;
    assert_eq!(c.cmd(&[b"JSON.TYPE", b"k"]).await, ss("array"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_type_string() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"\"hello\""]).await;
    assert_eq!(c.cmd(&[b"JSON.TYPE", b"k"]).await, ss("string"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_type_number() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"42"]).await;
    assert_eq!(c.cmd(&[b"JSON.TYPE", b"k"]).await, ss("number"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_type_boolean() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"true"]).await;
    assert_eq!(c.cmd(&[b"JSON.TYPE", b"k"]).await, ss("boolean"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_type_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"null"]).await;
    assert_eq!(c.cmd(&[b"JSON.TYPE", b"k"]).await, ss("null"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_type_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.TYPE", b"nonexistent"]).await,
        RespValue::Null
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_type_with_path() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":\"hello\"}"])
        .await;
    assert_eq!(c.cmd(&[b"JSON.TYPE", b"k", b"$.a"]).await, ss("string"));
    server.shutdown();
}

// ── JSON.STRLEN ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_strlen_string() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"\"hello\""]).await;
    assert_eq!(c.cmd(&[b"JSON.STRLEN", b"k"]).await, RespValue::Integer(5));
    server.shutdown();
}

#[tokio::test]
async fn test_json_strlen_nested_string() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"s\":\"world\"}"])
        .await;
    assert_eq!(
        c.cmd(&[b"JSON.STRLEN", b"k", b"$.s"]).await,
        RespValue::Integer(5)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_strlen_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.STRLEN", b"nonexistent"]).await,
        RespValue::Null
    );
    server.shutdown();
}

// ── JSON.OBJKEYS ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_objkeys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1,\"b\":2}"])
        .await;
    let resp = c.cmd(&[b"JSON.OBJKEYS", b"k"]).await;
    match resp {
        RespValue::Array(keys) => {
            let key_strs: Vec<String> = keys
                .into_iter()
                .map(|v| match v {
                    RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
                    other => panic!("expected BulkString, got {other:?}"),
                })
                .collect();
            assert_eq!(key_strs.len(), 2);
            assert!(key_strs.contains(&"a".to_string()));
            assert!(key_strs.contains(&"b".to_string()));
        }
        other => panic!("JSON.OBJKEYS should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_objkeys_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.OBJKEYS", b"nonexistent"]).await,
        RespValue::Null
    );
    server.shutdown();
}

// ── JSON.OBJLEN ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_objlen() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1,\"b\":2,\"c\":3}"])
        .await;
    assert_eq!(c.cmd(&[b"JSON.OBJLEN", b"k"]).await, RespValue::Integer(3));
    server.shutdown();
}

#[tokio::test]
async fn test_json_objlen_empty_object() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{}"]).await;
    assert_eq!(c.cmd(&[b"JSON.OBJLEN", b"k"]).await, RespValue::Integer(0));
    server.shutdown();
}

#[tokio::test]
async fn test_json_objlen_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.OBJLEN", b"nonexistent"]).await,
        RespValue::Null
    );
    server.shutdown();
}

// ── JSON.ARRLEN ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_arrlen() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3]"]).await;
    assert_eq!(c.cmd(&[b"JSON.ARRLEN", b"k"]).await, RespValue::Integer(3));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrlen_empty_array() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[]"]).await;
    assert_eq!(c.cmd(&[b"JSON.ARRLEN", b"k"]).await, RespValue::Integer(0));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrlen_nested() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2]}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRLEN", b"k", b"$.a"]).await,
        RespValue::Integer(2)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrlen_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRLEN", b"nonexistent"]).await,
        RespValue::Null
    );
    server.shutdown();
}

// ── JSON.ARRAPPEND ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_arrappend() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRAPPEND", b"k", b"$", b"3", b"4"]).await,
        RespValue::Integer(4)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("3"));
            assert!(s.contains("4"));
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrappend_nested() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1]}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRAPPEND", b"k", b"$.a", b"2"]).await,
        RespValue::Integer(2)
    );
    server.shutdown();
}

// ── JSON.ARRPOP ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_arrpop_default() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3]"]).await;
    let resp = c.cmd(&[b"JSON.ARRPOP", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s.as_ref(), "3");
        }
        other => panic!("JSON.ARRPOP should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrpop_index() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3]"]).await;
    let resp = c.cmd(&[b"JSON.ARRPOP", b"k", b"$", b"0"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s.as_ref(), "1");
        }
        other => panic!("JSON.ARRPOP should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrpop_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRPOP", b"nonexistent"]).await,
        RespValue::Null
    );
    server.shutdown();
}

// ── JSON.ARRINSERT ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_arrinsert() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,3]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRINSERT", b"k", b"$", b"1", b"2"]).await,
        RespValue::Integer(3)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("2"));
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrinsert_at_beginning() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[2,3]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRINSERT", b"k", b"$", b"0", b"1"]).await,
        RespValue::Integer(3)
    );
    server.shutdown();
}

// ── JSON.ARRTRIM ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_arrtrim() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3,4,5]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRTRIM", b"k", b"$", b"1", b"3"]).await,
        RespValue::Integer(3)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("2"));
            assert!(s.contains("3"));
            assert!(s.contains("4"));
            assert!(!s.contains("1"));
            assert!(!s.contains("5"));
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrtrim_to_empty() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRTRIM", b"k", b"$", b"5", b"10"]).await,
        RespValue::Integer(0)
    );
    server.shutdown();
}

// ── JSON.ARRINDEX ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_arrindex_found() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3,2]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRINDEX", b"k", b"$", b"2"]).await,
        RespValue::Integer(1)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrindex_not_found() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRINDEX", b"k", b"$", b"5"]).await,
        RespValue::Integer(-1)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrindex_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.ARRINDEX", b"nonexistent", b"$", b"1"]).await,
        RespValue::Null
    );
    server.shutdown();
}

// ── JSON.CLEAR ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_clear_object() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1,\"b\":2}"])
        .await;
    assert_eq!(
        c.cmd(&[b"JSON.CLEAR", b"k", b"$"]).await,
        RespValue::Integer(1)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s.as_ref(), "{}");
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_clear_array() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"[1,2,3]"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.CLEAR", b"k", b"$"]).await,
        RespValue::Integer(1)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s.as_ref(), "[]");
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_clear_missing_key_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.CLEAR", b"nonexistent", b"$"]).await,
        RespValue::Integer(0)
    );
    server.shutdown();
}

// ── JSON.MGET ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_mget_multiple_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k1", b"$", b"{\"a\":1}"]).await;
    c.cmd(&[b"JSON.SET", b"k2", b"$", b"{\"a\":2}"]).await;
    let resp = c.cmd(&[b"JSON.MGET", b"k1", b"k2", b"$.a"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        other => panic!("JSON.MGET should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_mget_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k1", b"$", b"{\"a\":1}"]).await;
    let resp = c.cmd(&[b"JSON.MGET", b"k1", b"nonexistent", b"$.a"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[1], RespValue::Null);
        }
        other => panic!("JSON.MGET should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── JSON.TOGGLE ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_toggle() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"b\":true}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.TOGGLE", b"k", b"$.b"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(
        c.cmd(&[b"JSON.TOGGLE", b"k", b"$.b"]).await,
        RespValue::Integer(1)
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_toggle_missing_key_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"JSON.TOGGLE", b"nonexistent", b"$.b"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "expected error for missing key, got {resp:?}"
    );
    server.shutdown();
}

// ── JSON.STRAPPEND ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_strappend() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"\"hello\""]).await;
    assert_eq!(
        c.cmd(&[b"JSON.STRAPPEND", b"k", b"$", b"\" world\""]).await,
        RespValue::Integer(11)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("hello world"));
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_strappend_missing_key_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.STRAPPEND", b"nonexistent", b"$", b"\"x\""])
            .await,
        RespValue::Null
    );
    server.shutdown();
}

// ── JSON.NUMMULTBY ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_nummultby() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"10"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.NUMMULTBY", b"k", b"$", b"3"]).await,
        bs(b"30")
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_nummultby_negative() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"10"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.NUMMULTBY", b"k", b"$", b"-2"]).await,
        bs(b"-20")
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_nummultby_nested() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"n\":5}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.NUMMULTBY", b"k", b"$.n", b"10"]).await,
        bs(b"50")
    );
    server.shutdown();
}

// ── JSON.MERGE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_json_merge_object() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.MERGE", b"k", b"$", b"{\"b\":2}"]).await,
        RespValue::Integer(1)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("a"));
            assert!(s.contains("b"));
        }
        other => panic!("JSON.GET should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_merge_missing_key_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.MERGE", b"nonexistent", b"$", b"{\"a\":1}"])
            .await,
        RespValue::Integer(0)
    );
    server.shutdown();
}

// ============================================================
// JSON.SET — NX/XX conditions
// ============================================================

#[tokio::test]
async fn test_json_set_nx_root_key_absent() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$", b"1", b"NX"]).await,
        ss("OK")
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    assert_eq!(resp, bs(b"1"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_nx_root_key_exists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"1"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$", b"2", b"NX"]).await,
        RespValue::Null
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    assert_eq!(resp, bs(b"1"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_xx_root_key_exists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"1"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$", b"2", b"XX"]).await,
        ss("OK")
    );
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    assert_eq!(resp, bs(b"2"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_xx_root_key_absent() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$", b"2", b"XX"]).await,
        RespValue::Null
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_nx_nonroot_path_absent() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$.b", b"2", b"NX"]).await,
        ss("OK")
    );
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.b"]).await;
    assert_eq!(resp, bs(b"2"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_nx_nonroot_path_exists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$.a", b"2", b"NX"]).await,
        RespValue::Null
    );
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a"]).await;
    assert_eq!(resp, bs(b"1"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_xx_nonroot_path_exists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$.a", b"2", b"XX"]).await,
        ss("OK")
    );
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a"]).await;
    assert_eq!(resp, bs(b"2"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_xx_nonroot_path_absent() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$.b", b"2", b"XX"]).await,
        RespValue::Null
    );
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_auto_create_nested_path() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"JSON.SET", b"k", b"$.a.b.c", b"42"]).await,
        ss("OK")
    );
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a.b.c"]).await;
    assert_eq!(resp, bs(b"42"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_set_wrong_type_on_string_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.SET", b"k", b"$.f", b"1"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.GET — multiple paths, wrong type
// ============================================================

#[tokio::test]
async fn test_json_get_multiple_paths() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1,\"b\":2}"])
        .await;
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a", b"$.b"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("a"));
            assert!(s.contains("b"));
        }
        other => panic!("expected BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_get_wrong_type_on_string_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.DEL — multiple paths, wrong type
// ============================================================

#[tokio::test]
async fn test_json_del_multiple_paths() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1,\"b\":2,\"c\":3}"])
        .await;
    let resp = c.cmd(&[b"JSON.DEL", b"k", b"$.a", b"$.c"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("b"));
            assert!(!s.contains("a"));
            assert!(!s.contains("c"));
        }
        other => panic!("expected BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_del_wrong_type_on_string_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.DEL", b"k"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.TOGGLE — non-boolean value, path doesn't exist
// ============================================================

#[tokio::test]
async fn test_json_toggle_non_boolean_value() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":42}"]).await;
    let resp = c.cmd(&[b"JSON.TOGGLE", b"k", b"$.a"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_toggle_path_not_exists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":true}"]).await;
    let resp = c.cmd(&[b"JSON.TOGGLE", b"k", b"$.b"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.MERGE — type mismatch, array merge, invalid value
// ============================================================

#[tokio::test]
async fn test_json_merge_type_mismatch() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    let resp = c.cmd(&[b"JSON.MERGE", b"k", b"$", b"[1,2,3]"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_merge_arrays() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2]}"]).await;
    assert_eq!(
        c.cmd(&[b"JSON.MERGE", b"k", b"$.a", b"[3,4]"]).await,
        RespValue::Integer(1)
    );
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("1"));
            assert!(s.contains("4"));
        }
        other => panic!("expected BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_merge_invalid_value_not_object_or_array() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    let resp = c.cmd(&[b"JSON.MERGE", b"k", b"$.a", b"42"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.ARRAPPEND — non-existent key, empty array
// ============================================================

#[tokio::test]
async fn test_json_arrappend_nonexistent_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c
        .cmd(&[b"JSON.ARRAPPEND", b"k", b"$", b"1", b"2", b"3"])
        .await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = c.cmd(&[b"JSON.GET", b"k"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("1"));
            assert!(s.contains("3"));
        }
        other => panic!("expected BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrappend_to_empty_array() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[]}"]).await;
    let resp = c.cmd(&[b"JSON.ARRAPPEND", b"k", b"$.a", b"42"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    server.shutdown();
}

// ============================================================
// JSON.ARRINSERT — negative index, missing key
// ============================================================

#[tokio::test]
async fn test_json_arrinsert_negative_index() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2,3]}"]).await;
    let resp = c
        .cmd(&[b"JSON.ARRINSERT", b"k", b"$.a", b"-1", b"99"])
        .await;
    assert_eq!(resp, RespValue::Integer(4));
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("99"));
            assert!(s.contains("3"));
        }
        other => panic!("expected BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrinsert_missing_key_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"JSON.ARRINSERT", b"k", b"$.a", b"0", b"1"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.ARRPOP — negative index, out-of-bounds, empty array
// ============================================================

#[tokio::test]
async fn test_json_arrpop_negative_index() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[10,20,30]}"])
        .await;
    let resp = c.cmd(&[b"JSON.ARRPOP", b"k", b"$.a", b"-1"]).await;
    assert_eq!(resp, bs(b"30"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrpop_out_of_bounds_index() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2]}"]).await;
    let resp = c.cmd(&[b"JSON.ARRPOP", b"k", b"$.a", b"100"]).await;
    assert_eq!(resp, RespValue::Null);
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrpop_empty_array() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[]}"]).await;
    let resp = c.cmd(&[b"JSON.ARRPOP", b"k", b"$.a"]).await;
    assert_eq!(resp, RespValue::Null);
    server.shutdown();
}

// ============================================================
// JSON.ARRTRIM — negative indices, start > stop
// ============================================================

#[tokio::test]
async fn test_json_arrtrim_negative_indices() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2,3,4,5]}"])
        .await;
    let resp = c.cmd(&[b"JSON.ARRTRIM", b"k", b"$.a", b"-3", b"-2"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a"]).await;
    match resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("3"));
            assert!(s.contains("4"));
        }
        other => panic!("expected BulkString, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrtrim_start_greater_than_stop() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2,3]}"]).await;
    let resp = c.cmd(&[b"JSON.ARRTRIM", b"k", b"$.a", b"2", b"1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrtrim_missing_key_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c
        .cmd(&[b"JSON.ARRTRIM", b"nonexistent", b"$.a", b"0", b"1"])
        .await;
    assert_eq!(resp, RespValue::Null);
    server.shutdown();
}

// ============================================================
// JSON.ARRINDEX — with bounds, negative indices, non-array
// ============================================================

#[tokio::test]
async fn test_json_arrindex_with_bounds() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[10,20,10,20]}"])
        .await;
    // Search for 10 in range [1..3] — should find index 2
    let resp = c
        .cmd(&[b"JSON.ARRINDEX", b"k", b"$.a", b"10", b"1", b"3"])
        .await;
    assert_eq!(resp, RespValue::Integer(2));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrindex_negative_start() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[10,20,30]}"])
        .await;
    // Negative start: -2 means start at index 1 (len + (-2) = 1)
    let resp = c.cmd(&[b"JSON.ARRINDEX", b"k", b"$.a", b"20", b"-2"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrindex_non_array_returns_null() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":\"string\"}"])
        .await;
    let resp = c.cmd(&[b"JSON.ARRINDEX", b"k", b"$.a", b"1"]).await;
    assert_eq!(resp, RespValue::Null);
    server.shutdown();
}

// ============================================================
// JSON.ARRLEN — non-array target
// ============================================================

#[tokio::test]
async fn test_json_arrlen_non_array_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":\"string\"}"])
        .await;
    let resp = c.cmd(&[b"JSON.ARRLEN", b"k", b"$.a"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.NUMINCRBY — float result, missing key, non-numeric
// ============================================================

#[tokio::test]
async fn test_json_numincrby_float_result() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"n\":10}"]).await;
    let resp = c.cmd(&[b"JSON.NUMINCRBY", b"k", b"$.n", b"0.5"]).await;
    assert_eq!(resp, bs(b"10.5"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_numincrby_missing_key_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c
        .cmd(&[b"JSON.NUMINCRBY", b"nonexistent", b"$.n", b"1"])
        .await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_numincrby_non_numeric_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":\"string\"}"])
        .await;
    let resp = c.cmd(&[b"JSON.NUMINCRBY", b"k", b"$.a", b"1"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.NUMMULTBY — float result, missing key, non-numeric
// ============================================================

#[tokio::test]
async fn test_json_nummultby_float_result() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"n\":3}"]).await;
    let resp = c.cmd(&[b"JSON.NUMMULTBY", b"k", b"$.n", b"0.5"]).await;
    assert_eq!(resp, bs(b"1.5"));
    server.shutdown();
}

#[tokio::test]
async fn test_json_nummultby_missing_key_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c
        .cmd(&[b"JSON.NUMMULTBY", b"nonexistent", b"$.n", b"1"])
        .await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_nummultby_non_numeric_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":\"string\"}"])
        .await;
    let resp = c.cmd(&[b"JSON.NUMMULTBY", b"k", b"$.a", b"1"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.STRAPPEND — non-string value, wrong type
// ============================================================

#[tokio::test]
async fn test_json_strappend_non_string_value() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":42}"]).await;
    let resp = c
        .cmd(&[b"JSON.STRAPPEND", b"k", b"$.a", b"\"extra\""])
        .await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_strappend_wrong_type_on_string_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.STRAPPEND", b"k", b"$.a", b"\"x\""]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.STRLEN — non-string value
// ============================================================

#[tokio::test]
async fn test_json_strlen_non_string_value() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":42}"]).await;
    let resp = c.cmd(&[b"JSON.STRLEN", b"k", b"$.a"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.OBJKEYS/OBJLEN — non-object target
// ============================================================

#[tokio::test]
async fn test_json_objkeys_non_object_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2]}"]).await;
    let resp = c.cmd(&[b"JSON.OBJKEYS", b"k", b"$.a"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_objlen_non_object_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":[1,2]}"]).await;
    let resp = c.cmd(&[b"JSON.OBJLEN", b"k", b"$.a"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ============================================================
// JSON.CLEAR — scalar values, null
// ============================================================

#[tokio::test]
async fn test_json_clear_string_value() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":\"hello\"}"])
        .await;
    let resp = c.cmd(&[b"JSON.CLEAR", b"k", b"$.a"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = c.cmd(&[b"JSON.GET", b"k", b"$.a"]).await;
    assert_eq!(resp, bs(b""));
    server.shutdown();
}

#[tokio::test]
async fn test_json_clear_null_value_noop() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":null}"]).await;
    let resp = c.cmd(&[b"JSON.CLEAR", b"k", b"$.a"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    server.shutdown();
}

// ============================================================
// JSON.MGET — non-JSON key in mix
// ============================================================

#[tokio::test]
async fn test_json_mget_with_non_json_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k1", b"$", b"{\"a\":1}"]).await;
    c.cmd(&[b"SET", b"k2", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.MGET", b"k1", b"k2", b"$"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            // k1 should have a result, k2 should be Null
            assert!(matches!(arr[1], RespValue::Null));
        }
        other => panic!("expected Array, got {other:?}"),
    }
    server.shutdown();
}

// ============================================================
// JSON.MGET — single key
// ============================================================

#[tokio::test]
async fn test_json_mget_single_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]).await;
    let resp = c.cmd(&[b"JSON.MGET", b"k", b"$"]).await;
    // MGET with a single key returns an error requiring multi-key lock
    assert!(
        matches!(resp, RespValue::Array(_)),
        "expected Array, got {resp:?}"
    );
    server.shutdown();
}

// ============================================================
// WrongType on non-JSON keys for remaining commands
// ============================================================

#[tokio::test]
async fn test_json_type_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.TYPE", b"k"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_toggle_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.TOGGLE", b"k", b"$.a"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_merge_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.MERGE", b"k", b"$", b"{\"a\":1}"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_clear_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.CLEAR", b"k"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrappend_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.ARRAPPEND", b"k", b"$.a", b"1"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_arrpop_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.ARRPOP", b"k"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_numincrby_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.NUMINCRBY", b"k", b"$.n", b"1"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

#[tokio::test]
async fn test_json_nummultby_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"notjson"]).await;
    let resp = c.cmd(&[b"JSON.NUMMULTBY", b"k", b"$.n", b"1"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}
