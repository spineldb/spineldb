mod common;

fn bs(s: &[u8]) -> spineldb::core::RespValue {
    spineldb::core::RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn bs_str(s: &str) -> spineldb::core::RespValue {
    spineldb::core::RespValue::BulkString(bytes::Bytes::copy_from_slice(s.as_bytes()))
}

fn ss(s: &str) -> spineldb::core::RespValue {
    spineldb::core::RespValue::SimpleString(s.to_string())
}

const NULL: spineldb::core::RespValue = spineldb::core::RespValue::Null;

#[tokio::test]
async fn test_multiple_concurrent_clients_isolated() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut a = common::Client::connect(addr).await;
    let mut b = common::Client::connect(addr).await;

    assert_eq!(a.cmd(&[b"CLIENT", b"SETNAME", b"client-a"]).await, ss("OK"),);
    assert_eq!(b.cmd(&[b"CLIENT", b"SETNAME", b"client-b"]).await, ss("OK"),);
    assert_eq!(a.cmd(&[b"CLIENT", b"GETNAME"]).await, bs_str("client-a"));
    assert_eq!(b.cmd(&[b"CLIENT", b"GETNAME"]).await, bs_str("client-b"));

    assert_eq!(a.cmd(&[b"SET", b"shared", b"from-a"]).await, ss("OK"));
    assert_eq!(b.cmd(&[b"GET", b"shared"]).await, bs(b"from-a"));

    assert_eq!(a.cmd(&[b"SELECT", b"2"]).await, ss("OK"));
    assert_eq!(a.cmd(&[b"SET", b"k", b"in-db-2"]).await, ss("OK"));
    assert_eq!(b.cmd(&[b"GET", b"k"]).await, NULL);
    assert_eq!(b.cmd(&[b"GET", b"shared"]).await, bs(b"from-a"));
    server.shutdown();
}

#[tokio::test]
async fn test_many_pipelined_commands_on_one_client() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut client = common::Client::connect(addr).await;
    for i in 0..10 {
        let key = format!("k{i}");
        let val = format!("v{i}");
        client.send(&[b"SET", key.as_bytes(), val.as_bytes()]).await;
    }
    for i in 0..10 {
        let resp = client.read_response().await;
        assert_eq!(resp, ss("OK"), "response {i} should be +OK");
    }
    for i in 0..10 {
        let key = format!("k{i}");
        let val = format!("v{i}");
        assert_eq!(
            client.cmd(&[b"GET", key.as_bytes()]).await,
            bs(val.as_bytes()),
        );
    }
    server.shutdown();
}
