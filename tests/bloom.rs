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

// ── BF.ADD on non-existent key (auto-creation) ──────────────────────────

#[tokio::test]
async fn test_bloom_add_autocreate() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    // BF.ADD on non-existent key should auto-create the filter
    assert_eq!(c.cmd(&[b"BF.ADD", b"newfilter", b"item1"]).await, int(1));
    assert_eq!(c.cmd(&[b"BF.ADD", b"newfilter", b"item1"]).await, int(0));
    assert_eq!(c.cmd(&[b"BF.EXISTS", b"newfilter", b"item1"]).await, int(1));
    server.shutdown();
}

// ── BF.EXISTS on non-existent key ──────────────────────────────────────

#[tokio::test]
async fn test_bloom_exists_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"BF.EXISTS", b"nonexistent", b"item1"]).await,
        int(0)
    );
    server.shutdown();
}

// ── BF.INFO on non-existent key ────────────────────────────────────────

#[tokio::test]
async fn test_bloom_info_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BF.INFO", b"nonexistent"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── BF.CARD on non-existent key ────────────────────────────────────────

#[tokio::test]
async fn test_bloom_card_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(c.cmd(&[b"BF.CARD", b"nonexistent"]).await, int(0));
    server.shutdown();
}

// ── BF.INSERT on existing filter without options ───────────────────────

#[tokio::test]
async fn test_bloom_insert_adds_to_existing() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"BF.RESERVE", b"f", b"0.01", b"1000"]).await;
    c.cmd(&[b"BF.ADD", b"f", b"old"]).await;
    // INSERT without options on existing filter
    let resp = c
        .cmd(&[b"BF.INSERT", b"f", b"ITEMS", b"new1", b"new2"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![int(1), int(1)]);
        }
        other => panic!("expected Array, got {other:?}"),
    }
    assert_eq!(c.cmd(&[b"BF.EXISTS", b"f", b"old"]).await, int(1));
    assert_eq!(c.cmd(&[b"BF.EXISTS", b"f", b"new1"]).await, int(1));
    server.shutdown();
}

// ── BF.INSERT with options on existing filter → error ──────────────────

#[tokio::test]
async fn test_bloom_insert_options_on_existing_filter() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"BF.RESERVE", b"f", b"0.01", b"1000"]).await;
    let resp = c
        .cmd(&[b"BF.INSERT", b"f", b"CAPACITY", b"2000", b"ITEMS", b"x"])
        .await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── BF.RESERVE validation: error_rate=0 → error ───────────────────────

#[tokio::test]
async fn test_bloom_reserve_invalid_error_rate_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BF.RESERVE", b"f", b"0", b"1000"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── BF.RESERVE validation: error_rate=1 → error ──────────────────────

#[tokio::test]
async fn test_bloom_reserve_invalid_error_rate_one() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BF.RESERVE", b"f", b"1", b"1000"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── BF.RESERVE validation: capacity=0 → error ─────────────────────────

#[tokio::test]
async fn test_bloom_reserve_zero_capacity() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BF.RESERVE", b"f", b"0.01", b"0"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── BF.RESERVE duplicate key → error ──────────────────────────────────

#[tokio::test]
async fn test_bloom_reserve_duplicate_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"BF.RESERVE", b"f", b"0.01", b"100"]).await,
        ss("OK")
    );
    let resp = c.cmd(&[b"BF.RESERVE", b"f", b"0.01", b"200"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── WrongType: BF.ADD on string key ───────────────────────────────────

#[tokio::test]
async fn test_bloom_add_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"stringvalue"]).await;
    let resp = c.cmd(&[b"BF.ADD", b"k", b"item"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── WrongType: BF.EXISTS on string key ────────────────────────────────

#[tokio::test]
async fn test_bloom_exists_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"SET", b"k", b"stringvalue"]).await;
    let resp = c.cmd(&[b"BF.EXISTS", b"k", b"item"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── WrongType: BF.MADD on list key ────────────────────────────────────

#[tokio::test]
async fn test_bloom_madd_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"LPUSH", b"k", b"v1"]).await;
    let resp = c.cmd(&[b"BF.MADD", b"k", b"a", b"b"]).await;
    assert!(matches!(resp, RespValue::Error(_)));
    server.shutdown();
}

// ── BF.MADD on non-existent key (auto-create) ─────────────────────────

#[tokio::test]
async fn test_bloom_madd_autocreate() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BF.MADD", b"newf", b"a", b"b", b"c"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![int(1), int(1), int(1)]);
        }
        other => panic!("expected Array, got {other:?}"),
    }
    server.shutdown();
}

// ── BF.MEXISTS on non-existent key ────────────────────────────────────

#[tokio::test]
async fn test_bloom_mexists_missing_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BF.MEXISTS", b"nonexistent", b"a", b"b"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![int(0), int(0)]);
        }
        other => panic!("expected Array, got {other:?}"),
    }
    server.shutdown();
}

// ── BF.INFO returns correct fields ────────────────────────────────────

#[tokio::test]
async fn test_bloom_info_returns_correct_fields() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"BF.RESERVE", b"f", b"0.01", b"1000"]).await;
    c.cmd(&[b"BF.ADD", b"f", b"item1"]).await;
    c.cmd(&[b"BF.ADD", b"f", b"item2"]).await;
    let resp = c.cmd(&[b"BF.INFO", b"f"]).await;
    match resp {
        RespValue::Array(arr) => {
            // BF.INFO returns 8 elements: 4 key-value pairs
            assert_eq!(arr.len(), 8);
            // Check that "Capacity" key is present
            let key_str = match &arr[0] {
                RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                other => panic!("expected BulkString or SimpleString for key, got {other:?}"),
            };
            assert_eq!(key_str, "Capacity");
            // Check that "Number of items inserted" value is 2
            match &arr[7] {
                RespValue::Integer(n) => assert_eq!(*n, 2),
                other => panic!("expected Integer for items_inserted, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
    server.shutdown();
}

// ── BF.CARD after adding items ────────────────────────────────────────

#[tokio::test]
async fn test_bloom_card_after_adds() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"BF.RESERVE", b"f", b"0.01", b"1000"]).await;
    c.cmd(&[b"BF.ADD", b"f", b"a"]).await;
    c.cmd(&[b"BF.ADD", b"f", b"b"]).await;
    c.cmd(&[b"BF.ADD", b"f", b"c"]).await;
    assert_eq!(c.cmd(&[b"BF.CARD", b"f"]).await, int(3));
    // Adding same item again doesn't increment
    c.cmd(&[b"BF.ADD", b"f", b"a"]).await;
    assert_eq!(c.cmd(&[b"BF.CARD", b"f"]).await, int(3));
    server.shutdown();
}

// ── BF.INSERT minimal syntax (just ITEMS) ─────────────────────────────

#[tokio::test]
async fn test_bloom_insert_minimal_syntax() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BF.INSERT", b"f", b"ITEMS", b"x", b"y"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![int(1), int(1)]);
        }
        other => panic!("expected Array, got {other:?}"),
    }
    // Verify the filter was created with default params
    assert_eq!(c.cmd(&[b"BF.EXISTS", b"f", b"x"]).await, int(1));
    server.shutdown();
}
