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

#[tokio::test]
async fn test_type_for_string_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"k", b"v"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"TYPE", b"k"]).await, ss("string"));
    server.shutdown();
}

#[tokio::test]
async fn test_type_for_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"TYPE", b"absent"]).await, ss("none"));
    server.shutdown();
}

#[tokio::test]
async fn test_type_returns_correct_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    assert_eq!(client.cmd(&[b"SET", b"str", b"hello"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"TYPE", b"str"]).await, ss("string"));

    assert_eq!(
        client.cmd(&[b"LPUSH", b"lst", b"item1", b"item2"]).await,
        int(2)
    );
    assert_eq!(client.cmd(&[b"TYPE", b"lst"]).await, ss("list"));

    assert_eq!(
        client.cmd(&[b"SADD", b"st", b"member1", b"member2"]).await,
        int(2)
    );
    assert_eq!(client.cmd(&[b"TYPE", b"st"]).await, ss("set"));

    assert_eq!(
        client
            .cmd(&[b"ZADD", b"zst", b"1.0", b"member1", b"2.0", b"member2"])
            .await,
        int(2)
    );
    assert_eq!(client.cmd(&[b"TYPE", b"zst"]).await, ss("zset"));

    assert_eq!(
        client
            .cmd(&[b"HSET", b"hsh", b"f1", b"v1", b"f2", b"v2"])
            .await,
        int(2)
    );
    assert_eq!(client.cmd(&[b"TYPE", b"hsh"]).await, ss("hash"));

    assert_eq!(client.cmd(&[b"TYPE", b"nonexistent"]).await, ss("none"));

    server.shutdown();
}

#[tokio::test]
async fn test_keys_returns_matching() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"user:1", b"a"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"user:2", b"b"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"other", b"c"]).await, ss("OK"));
    let resp = client.cmd(&[b"KEYS", b"user:*"]).await;
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
    assert_eq!(keys, vec!["user:1".to_string(), "user:2".to_string()]);
    server.shutdown();
}

#[tokio::test]
async fn test_rename_moves_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"src", b"v"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"RENAME", b"src", b"dst"]).await, ss("OK"),);
    assert_eq!(client.cmd(&[b"GET", b"src"]).await, NULL);
    assert_eq!(client.cmd(&[b"GET", b"dst"]).await, bs(b"v"));
    server.shutdown();
}

#[tokio::test]
async fn test_rename_missing_source_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let resp = client.cmd(&[b"RENAME", b"absent", b"dst"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "RENAME of missing source should error, got {resp:?}",
    );
    server.shutdown();
}

#[tokio::test]
async fn test_expire_and_ttl() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"k", b"v"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"EXPIRE", b"k", b"100"]).await, int(1));
    let ttl = client.cmd(&[b"TTL", b"k"]).await;
    match ttl {
        RespValue::Integer(n) => {
            assert!(n > 0 && n <= 100, "TTL out of range: {n}");
        }
        other => panic!("TTL should return Integer, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_ttl_missing_key_returns_minus_two() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"TTL", b"absent"]).await, int(-2));
    server.shutdown();
}

#[tokio::test]
async fn test_ttl_key_without_expiry_returns_minus_one() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"k", b"v"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"TTL", b"k"]).await, int(-1));
    server.shutdown();
}

#[tokio::test]
async fn test_persist_removes_expiry() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"k", b"v"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"EXPIRE", b"k", b"100"]).await, int(1));
    assert_eq!(client.cmd(&[b"PERSIST", b"k"]).await, int(1));
    assert_eq!(client.cmd(&[b"TTL", b"k"]).await, int(-1));
    server.shutdown();
}

#[tokio::test]
async fn test_expire_sets_ttl() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    assert_eq!(client.cmd(&[b"SET", b"k1", b"v1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"EXPIRE", b"k1", b"100"]).await, int(1));

    let ttl = client.cmd(&[b"TTL", b"k1"]).await;
    match ttl {
        RespValue::Integer(t) => assert!(t > 0 && t <= 100),
        other => panic!("TTL should return Integer, got {:?}", other),
    }

    assert_eq!(client.cmd(&[b"PERSIST", b"k1"]).await, int(1));
    assert_eq!(client.cmd(&[b"TTL", b"k1"]).await, int(-1));

    server.shutdown();
}
