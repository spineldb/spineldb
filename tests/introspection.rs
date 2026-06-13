mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn extract_text_body(resp: RespValue) -> String {
    match resp {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        RespValue::VerbatimString(_format, b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("Expected BulkString or VerbatimString, got {other:?}"),
    }
}

#[tokio::test]
async fn test_info_returns_text_with_server_section() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let body = extract_text_body(client.cmd(&[b"INFO"]).await);
    assert!(
        body.contains("# Server"),
        "INFO missing # Server section: {body}"
    );
    assert!(
        body.contains("spineldb_version"),
        "INFO should report a spineldb_version field: {body}",
    );
    server.shutdown();
}

#[tokio::test]
async fn test_info_specific_section() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let body = extract_text_body(client.cmd(&[b"INFO", b"server"]).await);
    assert!(body.contains("# Server"), "INFO server body: {body}");
    assert!(
        !body.contains("# Keyspace"),
        "INFO server should not include Keyspace: {body}",
    );
    server.shutdown();
}

#[tokio::test]
async fn test_config_get_known_param() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let resp = client.cmd(&[b"CONFIG", b"GET", b"databases"]).await;
    match resp {
        RespValue::Map(entries) => {
            // RESP3 Map format
            assert_eq!(entries.len(), 1);
            let (k, v) = &entries[0];
            assert_eq!(*k, bs(b"databases"));
            assert_eq!(*v, bs(b"16"));
        }
        RespValue::Array(arr) => {
            // RESP2 flat array format
            assert_eq!(arr, vec![bs(b"databases"), bs(b"16")]);
        }
        other => panic!("CONFIG GET should return Map or Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_config_get_unknown_param_returns_empty_array() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let resp = client
        .cmd(&[b"CONFIG", b"GET", b"this_does_not_exist"])
        .await;
    assert_eq!(resp, RespValue::Array(vec![]));
    server.shutdown();
}
