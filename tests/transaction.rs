mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn ss(s: &str) -> RespValue {
    RespValue::SimpleString(s.to_string())
}

const NULL: RespValue = RespValue::Null;

#[tokio::test]
async fn test_multi_exec_basic() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    assert_eq!(client.cmd(&[b"MULTI"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"k1", b"v1"]).await, ss("QUEUED"));
    assert_eq!(client.cmd(&[b"SET", b"k2", b"v2"]).await, ss("QUEUED"));
    assert_eq!(client.cmd(&[b"GET", b"k1"]).await, ss("QUEUED"));

    let resp = client.cmd(&[b"EXEC"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], ss("OK"));
            assert_eq!(arr[1], ss("OK"));
            assert_eq!(arr[2], bs(b"v1"));
        }
        other => panic!("EXEC should return Array, got {:?}", other),
    }

    assert_eq!(client.cmd(&[b"GET", b"k1"]).await, bs(b"v1"));
    assert_eq!(client.cmd(&[b"GET", b"k2"]).await, bs(b"v2"));
    server.shutdown();
}

#[tokio::test]
async fn test_multi_exec_error_propagation() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    assert_eq!(client.cmd(&[b"SET", b"k1", b"string_val"]).await, ss("OK"));

    assert_eq!(client.cmd(&[b"MULTI"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"k2", b"val2"]).await, ss("QUEUED"));
    assert_eq!(client.cmd(&[b"LPUSH", b"k1", b"item"]).await, ss("QUEUED"));
    assert_eq!(client.cmd(&[b"SET", b"k3", b"val3"]).await, ss("QUEUED"));

    let resp = client.cmd(&[b"EXEC"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], ss("OK"));
            assert!(
                matches!(&arr[1], RespValue::Error(_)),
                "LPUSH on string should return error, got {:?}",
                arr[1]
            );
            assert!(
                matches!(&arr[2], RespValue::Error(e) if e.contains("EXECABORT")),
                "SET k3 should return EXECABORT, got {:?}",
                arr[2]
            );
        }
        other => panic!("EXEC should return Array, got {:?}", other),
    }

    assert_eq!(client.cmd(&[b"GET", b"k2"]).await, bs(b"val2"));
    assert_eq!(client.cmd(&[b"GET", b"k3"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_multi_exec_discard() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;

    assert_eq!(client.cmd(&[b"MULTI"]).await, ss("OK"));
    assert_eq!(client.cmd(&[b"SET", b"k1", b"v1"]).await, ss("QUEUED"));
    assert_eq!(client.cmd(&[b"DISCARD"]).await, ss("OK"));

    assert_eq!(client.cmd(&[b"GET", b"k1"]).await, NULL);
    server.shutdown();
}
