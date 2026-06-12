mod common;

use spineldb::core::RespValue;

fn ss(s: &str) -> RespValue {
    RespValue::SimpleString(s.to_string())
}

#[tokio::test]
async fn test_unknown_command_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let resp = client.cmd(&[b"DEFINITELY_NOT_A_REAL_COMMAND"]).await;
    let body = match resp {
        RespValue::Error(s) => s,
        other => panic!("expected Error, got {other:?}"),
    };
    assert!(
        body.contains("Unknown command"),
        "error body should mention 'Unknown command', got {body}",
    );
    server.shutdown();
}

#[tokio::test]
async fn test_wrong_arity_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let resp = client.cmd(&[b"SET", b"only-key"]).await;
    let body = match resp {
        RespValue::Error(s) => s,
        other => panic!("expected Error, got {other:?}"),
    };
    assert!(
        body.to_lowercase().contains("wrong number of arguments"),
        "error body should mention wrong number of arguments, got {body}",
    );
    server.shutdown();
}

#[tokio::test]
async fn test_incr_on_non_integer_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"SET", b"k", b"not-a-number"]).await, ss("OK"));
    let resp = client.cmd(&[b"INCR", b"k"]).await;
    let body = match resp {
        RespValue::Error(s) => s,
        other => panic!("expected Error, got {other:?}"),
    };
    assert!(
        body.to_lowercase().contains("not an integer"),
        "error body should say value is not an integer, got {body}",
    );
    server.shutdown();
}
