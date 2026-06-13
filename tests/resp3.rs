mod common;

use spineldb::core::RespValue;

#[tokio::test]
async fn test_hello_returns_map_in_resp3() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Negotiate RESP3 with HELLO 3
    let resp = client.cmd(&[b"HELLO", b"3"]).await;

    // HELLO 3 should return a Map in RESP3
    match &resp {
        RespValue::Map(entries) => {
            // Should contain server, version, proto, id, mode, role, modules, capa
            assert!(
                entries.len() >= 8,
                "HELLO Map should have at least 8 entries, got {}",
                entries.len()
            );
            // Verify proto=3
            let proto_entry = entries
                .iter()
                .find(|(k, _)| k == &RespValue::BulkString(bytes::Bytes::from_static(b"proto")));
            assert!(
                proto_entry.is_some(),
                "HELLO Map should contain 'proto' key"
            );
            if let Some((_, v)) = proto_entry {
                assert_eq!(*v, RespValue::Integer(3));
            }
        }
        other => panic!("HELLO 3 should return Map, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_hello_returns_array_in_resp2() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Negotiate RESP2 with HELLO 2
    let resp = client.cmd(&[b"HELLO", b"2"]).await;

    // HELLO 2 should return a flat Array in RESP2
    match &resp {
        RespValue::Array(arr) => {
            // Should have 16 elements (8 key-value pairs)
            assert_eq!(arr.len(), 16, "HELLO 2 Array should have 16 elements");
        }
        other => panic!("HELLO 2 should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_hgetall_returns_map_in_resp3() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Negotiate RESP3
    client.cmd(&[b"HELLO", b"3"]).await;

    // Set a hash
    client.cmd(&[b"HSET", b"myhash", b"f1", b"v1"]).await;
    client.cmd(&[b"HSET", b"myhash", b"f2", b"v2"]).await;

    let resp = client.cmd(&[b"HGETALL", b"myhash"]).await;

    match &resp {
        RespValue::Map(entries) => {
            assert_eq!(entries.len(), 2);
            // Verify entries contain f1=v1 and f2=v2
            let f1 = entries
                .iter()
                .find(|(k, _)| k == &RespValue::BulkString(bytes::Bytes::from_static(b"f1")));
            assert!(f1.is_some());
            if let Some((_, v)) = f1 {
                assert_eq!(*v, RespValue::BulkString(bytes::Bytes::from_static(b"v1")));
            }
            let f2 = entries
                .iter()
                .find(|(k, _)| k == &RespValue::BulkString(bytes::Bytes::from_static(b"f2")));
            assert!(f2.is_some());
            if let Some((_, v)) = f2 {
                assert_eq!(*v, RespValue::BulkString(bytes::Bytes::from_static(b"v2")));
            }
        }
        other => panic!("HGETALL in RESP3 should return Map, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_hgetall_returns_array_in_resp2() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Stay in default RESP3, then downgrade to RESP2
    client.cmd(&[b"HELLO", b"2"]).await;

    // Set a hash
    client.cmd(&[b"HSET", b"myhash", b"f1", b"v1"]).await;
    client.cmd(&[b"HSET", b"myhash", b"f2", b"v2"]).await;

    let resp = client.cmd(&[b"HGETALL", b"myhash"]).await;

    match &resp {
        RespValue::Array(arr) => {
            // RESP2 HGETALL returns flat array: [f1, v1, f2, v2]
            assert_eq!(arr.len(), 4);
        }
        other => panic!("HGETALL in RESP2 should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_client_list_reports_proto() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Default is RESP3
    let resp = client.cmd(&[b"CLIENT", b"LIST"]).await;
    if let RespValue::BulkString(b) = resp {
        let list_str = String::from_utf8_lossy(&b);
        // Should contain proto=3
        assert!(
            list_str.contains("proto=3"),
            "CLIENT LIST should contain proto=3 for new connection: {list_str}"
        );
    } else {
        panic!("CLIENT LIST should return BulkString");
    }

    // Downgrade to RESP2
    client.cmd(&[b"HELLO", b"2"]).await;

    let resp = client.cmd(&[b"CLIENT", b"LIST"]).await;
    if let RespValue::BulkString(b) = resp {
        let list_str = String::from_utf8_lossy(&b);
        // Should contain proto=2 after downgrade
        assert!(
            list_str.contains("proto=2"),
            "CLIENT LIST should contain proto=2 after HELLO 2: {list_str}"
        );
    } else {
        panic!("CLIENT LIST should return BulkString");
    }

    server.shutdown();
}

#[tokio::test]
async fn test_default_connection_is_resp3() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Without sending HELLO, the connection should default to RESP3
    // HGETALL should return Map (native RESP3 type)
    client.cmd(&[b"HSET", b"testhash", b"k", b"v"]).await;
    let resp = client.cmd(&[b"HGETALL", b"testhash"]).await;

    match &resp {
        RespValue::Map(_) => {
            // Correct - RESP3 default returns Map
        }
        other => {
            panic!("Default connection should be RESP3, HGETALL should return Map, got {other:?}")
        }
    }

    server.shutdown();
}

#[tokio::test]
async fn test_set_returns_ok_in_resp3() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Negotiate RESP3
    client.cmd(&[b"HELLO", b"3"]).await;

    let resp = client.cmd(&[b"SET", b"key1", b"value1"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".into()));

    let resp = client.cmd(&[b"GET", b"key1"]).await;
    assert_eq!(
        resp,
        RespValue::BulkString(bytes::Bytes::from_static(b"value1"))
    );

    server.shutdown();
}

#[tokio::test]
async fn test_switching_between_resp2_and_resp3() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    // Default is RESP3
    client.cmd(&[b"HSET", b"h", b"f", b"v"]).await;

    // Should be RESP3 - Map
    let resp = client.cmd(&[b"HGETALL", b"h"]).await;
    assert!(matches!(resp, RespValue::Map(_)), "Should be RESP3 Map");

    // Switch to RESP2
    client.cmd(&[b"HELLO", b"2"]).await;
    let resp = client.cmd(&[b"HGETALL", b"h"]).await;
    assert!(matches!(resp, RespValue::Array(_)), "Should be RESP2 Array");

    // Switch back to RESP3
    client.cmd(&[b"HELLO", b"3"]).await;
    let resp = client.cmd(&[b"HGETALL", b"h"]).await;
    assert!(
        matches!(resp, RespValue::Map(_)),
        "Should be RESP3 Map again"
    );

    server.shutdown();
}
