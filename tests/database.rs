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
async fn test_select_switches_db() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"k", b"in-db-0"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SELECT", b"1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"GET", b"k"]).await, NULL);
    assert_eq!(client.cmd(&[b"SELECT", b"0"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"GET", b"k"]).await, bs(b"in-db-0"));
    server.shutdown();
}

#[tokio::test]
async fn test_dbsize_empty_and_after_sets() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(0));
    assert_eq!(client.cmd(&[b"SET", b"a", b"1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"b", b"2"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(2));
    server.shutdown();
}

#[tokio::test]
async fn test_dbsize_tracks_inserts_and_deletes() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(0));

    assert_eq!(client.cmd(&[b"SET", b"k1", b"v1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"k2", b"v2"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(2));

    assert_eq!(client.cmd(&[b"DEL", b"k1"]).await, int(1));
    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(1));

    assert_eq!(client.cmd(&[b"DEL", b"no_such_key"]).await, int(0));
    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(1));

    server.shutdown();
}

#[tokio::test]
async fn test_flushdb_clears_current_db_only() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"k0", b"v"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SELECT", b"1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"k1", b"v"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"FLUSHDB"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(0));
    assert_eq!(client.cmd(&[b"SELECT", b"0"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"DBSIZE"]).await, int(1));
    server.shutdown();
}

#[tokio::test]
async fn test_flushall_clears_all_databases() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    assert_eq!(client.cmd(&[b"SET", b"k1", b"v1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SELECT", b"1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"k2", b"v2"]).await, ss("OK"));

    assert_eq!(client.cmd(&[b"FLUSHALL"]).await, ss("OK"));

    assert_eq!(client.cmd(&[b"SELECT", b"0"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"GET", b"k1"]).await, NULL);

    assert_eq!(client.cmd(&[b"SELECT", b"1"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"GET", b"k2"]).await, NULL);

    server.shutdown();
}

#[tokio::test]
async fn test_select_out_of_range_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let resp = client.cmd(&[b"SELECT", b"99"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "SELECT 99 should return an Error, got {resp:?}",
    );
    server.shutdown();
}

#[tokio::test]
async fn test_scan_pagination_returns_all_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    for i in 0..50 {
        let key = format!("scankey:{:03}", i);
        let val = format!("val{}", i);
        assert_eq!(
            client.cmd(&[b"SET", key.as_bytes(), val.as_bytes()]).await,
            ss("OK")
        );
    }

    let mut all_keys = std::collections::HashSet::new();
    let mut cursor: i64 = 0;
    loop {
        let cursor_str = cursor.to_string();
        let resp = client
            .cmd(&[b"SCAN", cursor_str.as_bytes(), b"COUNT", b"10"])
            .await;
        match resp {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2, "SCAN should return [cursor, keys]");
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

    assert_eq!(all_keys.len(), 50, "SCAN should return all 50 unique keys");
    for i in 0..50 {
        let expected = format!("scankey:{:03}", i);
        assert!(all_keys.contains(&expected), "missing key: {}", expected);
    }
    server.shutdown();
}

#[tokio::test]
async fn test_scan_empty_db_returns_cursor_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    let resp = client.cmd(&[b"SCAN", b"0"]).await;
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
