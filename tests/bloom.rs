mod common;

use spineldb::core::RespValue;

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

fn ss(s: &str) -> RespValue {
    RespValue::SimpleString(s.to_string())
}

// ── BF.RESERVE / BF.ADD / BF.EXISTS ─────────────────────────────────────

#[tokio::test]
async fn test_bloom_reserve_add_exists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    assert_eq!(
        c.cmd(&[b"BF.RESERVE", b"myfilter", b"0.01", b"1000"]).await,
        ss("OK")
    );

    assert_eq!(c.cmd(&[b"BF.ADD", b"myfilter", b"item1"]).await, int(1));
    assert_eq!(c.cmd(&[b"BF.ADD", b"myfilter", b"item1"]).await, int(0));
    assert_eq!(c.cmd(&[b"BF.ADD", b"myfilter", b"item2"]).await, int(1));

    assert_eq!(c.cmd(&[b"BF.EXISTS", b"myfilter", b"item1"]).await, int(1));
    assert_eq!(c.cmd(&[b"BF.EXISTS", b"myfilter", b"item2"]).await, int(1));
    assert_eq!(
        c.cmd(&[b"BF.EXISTS", b"myfilter", b"missing"]).await,
        int(0)
    );

    server.shutdown();
}

// ── BF.MADD ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_bloom_madd() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"BF.RESERVE", b"myfilter", b"0.01", b"1000"]).await;

    let resp = c.cmd(&[b"BF.MADD", b"myfilter", b"a", b"b", b"c"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![int(1), int(1), int(1)]);
        }
        other => panic!("BF.MADD should return Array, got {other:?}"),
    }

    let resp = c.cmd(&[b"BF.MADD", b"myfilter", b"a", b"d"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![int(0), int(1)]);
        }
        other => panic!("BF.MADD second call should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── BF.MEXISTS ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_bloom_mexists() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"BF.RESERVE", b"myfilter", b"0.01", b"1000"]).await;
    c.cmd(&[b"BF.ADD", b"myfilter", b"item1"]).await;

    let resp = c
        .cmd(&[b"BF.MEXISTS", b"myfilter", b"item1", b"item2"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![int(1), int(0)]);
        }
        other => panic!("BF.MEXISTS should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── BF.INFO / BF.CARD ────────────────────────────────────────────────────

#[tokio::test]
async fn test_bloom_info_card() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"BF.RESERVE", b"myfilter", b"0.01", b"1000"]).await;
    c.cmd(&[b"BF.ADD", b"myfilter", b"item1"]).await;

    let resp = c.cmd(&[b"BF.INFO", b"myfilter"]).await;
    match resp {
        RespValue::Array(_) => {}
        other => panic!("BF.INFO should return Array, got {other:?}"),
    }

    match c.cmd(&[b"BF.CARD", b"myfilter"]).await {
        RespValue::Integer(n) => assert!(n >= 1, "BF.CARD should be >= 1, got {n}"),
        other => panic!("BF.CARD should return Integer, got {other:?}"),
    }
    server.shutdown();
}

// ── BF.INSERT ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_bloom_insert() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let resp = c
        .cmd(&[
            b"BF.INSERT",
            b"newfilter",
            b"CAPACITY",
            b"1000",
            b"ERROR",
            b"0.01",
            b"ITEMS",
            b"item1",
            b"item2",
        ])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], int(1));
            assert_eq!(items[1], int(1));
        }
        other => panic!("BF.INSERT should return Array, got {other:?}"),
    }

    assert_eq!(c.cmd(&[b"BF.EXISTS", b"newfilter", b"item1"]).await, int(1));
    server.shutdown();
}
