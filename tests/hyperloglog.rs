mod common;

use spineldb::core::RespValue;

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

// ── PFADD / PFCOUNT ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_hll_pfadd_pfcount() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    assert_eq!(c.cmd(&[b"PFADD", b"hll", b"a", b"b", b"c"]).await, int(1));
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(3));

    assert_eq!(c.cmd(&[b"PFADD", b"hll", b"a", b"d", b"e"]).await, int(1));
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(5));

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfcount_multiple_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"PFADD", b"hll1", b"a", b"b"]).await;
    c.cmd(&[b"PFADD", b"hll2", b"c", b"d"]).await;

    let resp = c.cmd(&[b"PFCOUNT", b"hll1", b"hll2"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(n >= 4, "PFCOUNT should be at least 4, got {n}");
        }
        other => panic!("PFCOUNT should return Integer, got {other:?}"),
    }
    server.shutdown();
}

// ── PFMERGE ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_hll_pfmerge() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"PFADD", b"hll1", b"a", b"b"]).await;
    c.cmd(&[b"PFADD", b"hll2", b"c", b"d"]).await;

    assert_eq!(
        c.cmd(&[b"PFMERGE", b"hll_dest", b"hll1", b"hll2"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    let resp = c.cmd(&[b"PFCOUNT", b"hll_dest"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(
                n >= 4,
                "PFCOUNT after PFMERGE should be at least 4, got {n}"
            );
        }
        other => panic!("PFCOUNT should return Integer, got {other:?}"),
    }
    server.shutdown();
}
