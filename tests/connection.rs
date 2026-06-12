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

#[tokio::test]
async fn test_ping_no_arg() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"PING"]).await, ss("PONG"));
    server.shutdown();
}

#[tokio::test]
async fn test_ping_with_arg() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"PING", b"hello"]).await, bs(b"hello"));
    server.shutdown();
}

#[tokio::test]
async fn test_echo() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(
        client.cmd(&[b"ECHO", b"round-trip"]).await,
        bs(b"round-trip")
    );
    server.shutdown();
}

#[tokio::test]
async fn test_client_setname_getname() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(
        client.cmd(&[b"CLIENT", b"SETNAME", b"my-app"]).await,
        ss("OK"),
    );
    assert_eq!(client.cmd(&[b"CLIENT", b"GETNAME"]).await, bs_str("my-app"),);
    server.shutdown();
}

#[tokio::test]
async fn test_client_list_includes_self() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    let resp = client.cmd(&[b"CLIENT", b"LIST"]).await;
    let body = match resp {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("CLIENT LIST should return BulkString, got {other:?}"),
    };
    assert!(body.contains("addr=127.0.0.1"), "CLIENT LIST body: {body}");
    server.shutdown();
}

#[tokio::test]
async fn test_quit_closes_connection() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    assert_eq!(client.cmd(&[b"QUIT"]).await, ss("OK"));
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), client.read_frame()).await;
    let frame_result = result.unwrap_or_else(|_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "post-QUIT read timed out (connection still open?)",
        ))
    });
    assert!(
        frame_result.is_err(),
        "expected the post-QUIT read to fail with a closed connection, got {frame_result:?}",
    );
    server.shutdown();
}
