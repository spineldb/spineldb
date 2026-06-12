mod common;

use spineldb::core::RespValue;

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

// ── PUBLISH / SUBSCRIBE ──────────────────────────────────────────────────

#[tokio::test]
async fn test_pubsub_publish_no_subscribers() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"PUBLISH", b"channel", b"hello"]).await, int(0));
    server.shutdown();
}

#[tokio::test]
async fn test_pubsub_subscribe_returns_ok() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.send(&[b"SUBSCRIBE", b"mychannel"]).await;
    let resp = c.read_response().await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"subscribe"));
            assert_eq!(items[1], bs(b"mychannel"));
            assert_eq!(items[2], int(1));
        }
        other => panic!("SUBSCRIBE response should be Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_pubsub_psubscribe_returns_ok() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.send(&[b"PSUBSCRIBE", b"my*"]).await;
    let resp = c.read_response().await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"psubscribe"));
            assert_eq!(items[1], bs(b"my*"));
            assert_eq!(items[2], int(1));
        }
        other => panic!("PSUBSCRIBE response should be Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_pubsub_unsubscribe() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.send(&[b"SUBSCRIBE", b"ch1"]).await;
    c.read_response().await;

    c.send(&[b"UNSUBSCRIBE", b"ch1"]).await;
    let resp = c.read_response().await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"unsubscribe"));
            assert_eq!(items[1], bs(b"ch1"));
            assert_eq!(items[2], int(0));
        }
        other => panic!("UNSUBSCRIBE response should be Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_pubsub_punsubscribe() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.send(&[b"PSUBSCRIBE", b"ch*"]).await;
    c.read_response().await;

    c.send(&[b"PUNSUBSCRIBE", b"ch*"]).await;
    let resp = c.read_response().await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"punsubscribe"));
            assert_eq!(items[1], bs(b"ch*"));
            assert_eq!(items[2], int(0));
        }
        other => panic!("PUNSUBSCRIBE response should be Array, got {other:?}"),
    }
    server.shutdown();
}

// ── PUBSUB NUMSUB / NUMPAT ───────────────────────────────────────────────

#[tokio::test]
async fn test_pubsub_numsub() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let mut sub = common::Client::connect(addr).await;
    sub.send(&[b"SUBSCRIBE", b"ch1"]).await;
    sub.read_response().await;

    let resp = c.cmd(&[b"PUBSUB", b"NUMSUB", b"ch1", b"ch2"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], bs(b"ch1"));
            assert_eq!(items[1], int(1));
            assert_eq!(items[2], bs(b"ch2"));
            assert_eq!(items[3], int(0));
        }
        other => panic!("PUBSUB NUMSUB should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_pubsub_numpat() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let mut sub = common::Client::connect(addr).await;
    sub.send(&[b"PSUBSCRIBE", b"ch*"]).await;
    sub.read_response().await;

    let resp = c.cmd(&[b"PUBSUB", b"NUMPAT"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(n >= 1, "NUMPAT should be >= 1, got {n}");
        }
        other => panic!("PUBSUB NUMPAT should return Integer, got {other:?}"),
    }
    server.shutdown();
}
