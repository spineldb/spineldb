mod common;

use bytes::Bytes;
use spineldb::core::RespValue;

fn ok() -> RespValue {
    RespValue::SimpleString("OK".to_string())
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

fn null() -> RespValue {
    RespValue::Null
}

// ── VS.RESERVE ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_reserve_creates_index() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    assert_eq!(c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await, ok());

    // TYPE should return spinelvector
    assert_eq!(
        c.cmd(&[b"TYPE", b"myvec"]).await,
        RespValue::SimpleString("spinelvector".to_string())
    );

    server.shutdown();
}

#[tokio::test]
async fn test_vs_reserve_with_optional_params() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    assert_eq!(
        c.cmd(&[
            b"VS.RESERVE",
            b"myvec",
            b"4",
            b"COSINE",
            b"CAPACITY",
            b"5000",
            b"M",
            b"32",
            b"EF_CONSTRUCTION",
            b"400"
        ])
        .await,
        ok()
    );

    server.shutdown();
}

#[tokio::test]
async fn test_vs_reserve_duplicate_key_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    let resp = c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    match resp {
        RespValue::Error(_) => {}
        other => panic!("expected error, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.ADD ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_add_and_get() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    assert_eq!(
        c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0"])
            .await,
        ok()
    );

    // GET should return the vector
    let resp = c.cmd(&[b"VS.GET", b"myvec", b"v1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3); // id, vector, metadata
            assert_eq!(items[0], RespValue::BulkString(Bytes::from_static(b"v1")));
        }
        other => panic!("VS.GET should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_add_with_metadata() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    assert_eq!(
        c.cmd(&[
            b"VS.ADD",
            b"myvec",
            b"v1",
            b"1.0",
            b"2.0",
            b"3.0",
            b"METADATA",
            b"some info"
        ])
        .await,
        ok()
    );

    let resp = c.cmd(&[b"VS.GET", b"myvec", b"v1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(
                items[2],
                RespValue::BulkString(Bytes::from_static(b"some info"))
            );
        }
        other => panic!("VS.GET should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_add_dimension_mismatch() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    let resp = c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0"]).await;
    match resp {
        RespValue::Error(_) => {}
        other => panic!("expected error for dimension mismatch, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_add_wrong_type_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"SET", b"mykey", b"hello"]).await;

    let resp = c
        .cmd(&[b"VS.ADD", b"mykey", b"v1", b"1.0", b"2.0", b"3.0"])
        .await;
    match resp {
        RespValue::Error(_) => {}
        other => panic!("expected error for wrong type, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.MADD ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_madd_batch() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    let resp = c
        .cmd(&[
            b"VS.MADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0", b"v2", b"4.0", b"5.0", b"6.0",
        ])
        .await;

    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], ok());
            assert_eq!(items[1], ok());
        }
        other => panic!("VS.MADD should return Array, got {other:?}"),
    }

    // Check cardinality
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(2));

    server.shutdown();
}

// ── VS.DEL ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_del_removes_vector() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0"])
        .await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v2", b"4.0", b"5.0", b"6.0"])
        .await;

    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(2));

    // Delete one
    assert_eq!(c.cmd(&[b"VS.DEL", b"myvec", b"v1"]).await, int(1));

    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(1));

    // GET deleted should return nil
    assert_eq!(c.cmd(&[b"VS.GET", b"myvec", b"v1"]).await, null());

    // GET remaining should work
    let resp = c.cmd(&[b"VS.GET", b"myvec", b"v2"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items[0], RespValue::BulkString(Bytes::from_static(b"v2")));
        }
        other => panic!("VS.GET should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_del_nonexistent() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    assert_eq!(c.cmd(&[b"VS.DEL", b"myvec", b"missing"]).await, int(0));

    server.shutdown();
}

// ── VS.SEARCH ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_search_returns_nearest() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"2", b"L2"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"origin", b"0.0", b"0.0"])
        .await;
    c.cmd(&[b"VS.ADD", b"myvec", b"far", b"10.0", b"10.0"])
        .await;

    let resp = c
        .cmd(&[b"VS.SEARCH", b"myvec", b"0.1", b"0.0", b"COUNT", b"1"])
        .await;

    match resp {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 1);
            // Each result is [id, distance, vector, metadata]
            match &results[0] {
                RespValue::Array(item) => {
                    assert_eq!(
                        item[0],
                        RespValue::BulkString(Bytes::from_static(b"origin"))
                    );
                }
                other => panic!("result should be Array, got {other:?}"),
            }
        }
        other => panic!("VS.SEARCH should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_search_respects_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"2", b"L2"]).await;
    for i in 0..10 {
        let x = format!("{}.0", i);
        let id = format!("v{}", i);
        c.cmd(&[b"VS.ADD", b"myvec", id.as_bytes(), x.as_bytes(), b"0.0"])
            .await;
    }

    let resp = c
        .cmd(&[b"VS.SEARCH", b"myvec", b"0.0", b"0.0", b"COUNT", b"3"])
        .await;

    match resp {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 3);
        }
        other => panic!("VS.SEARCH should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_search_cosine() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"2", b"COSINE"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"east", b"1.0", b"0.0"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"north", b"0.0", b"1.0"])
        .await;

    let resp = c
        .cmd(&[b"VS.SEARCH", b"myvec", b"1.0", b"0.1", b"COUNT", b"1"])
        .await;

    match resp {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 1);
            match &results[0] {
                RespValue::Array(item) => {
                    assert_eq!(item[0], RespValue::BulkString(Bytes::from_static(b"east")));
                }
                other => panic!("result should be Array, got {other:?}"),
            }
        }
        other => panic!("VS.SEARCH should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_search_empty_index() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    let resp = c
        .cmd(&[
            b"VS.SEARCH",
            b"myvec",
            b"1.0",
            b"2.0",
            b"3.0",
            b"COUNT",
            b"10",
        ])
        .await;

    match resp {
        RespValue::Array(results) => {
            assert!(results.is_empty());
        }
        other => panic!("VS.SEARCH should return empty Array, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.INFO ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_info_returns_metadata() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[
        b"VS.RESERVE",
        b"myvec",
        b"4",
        b"COSINE",
        b"CAPACITY",
        b"5000",
        b"M",
        b"32",
    ])
    .await;

    let resp = c.cmd(&[b"VS.INFO", b"myvec"]).await;
    match resp {
        RespValue::Array(items) => {
            // Should be flat array of field-value pairs
            assert!(!items.is_empty());
            assert!(items.len() % 2 == 0);
        }
        other => panic!("VS.INFO should return Array, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.CARD ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_card_returns_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(0));

    c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0"])
        .await;
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(1));

    c.cmd(&[b"VS.ADD", b"myvec", b"v2", b"4.0", b"5.0", b"6.0"])
        .await;
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(2));

    c.cmd(&[b"VS.DEL", b"myvec", b"v1"]).await;
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(1));

    server.shutdown();
}

// ── VS.EXISTS ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_exists_returns_1_for_existing() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0"])
        .await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v2", b"4.0", b"5.0", b"6.0"])
        .await;

    let resp = c.cmd(&[b"VS.EXISTS", b"myvec", b"v1", b"v3", b"v2"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], int(1)); // v1 exists
            assert_eq!(items[1], int(0)); // v3 does not exist
            assert_eq!(items[2], int(1)); // v2 exists
        }
        other => panic!("VS.EXISTS should return Array, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.REBUILD ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_rebuild_returns_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0"])
        .await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v2", b"4.0", b"5.0", b"6.0"])
        .await;

    let resp = c.cmd(&[b"VS.REBUILD", b"myvec"]).await;
    // Should return the count of vectors
    assert_eq!(resp, int(2));

    // Search should still work after rebuild
    let resp = c
        .cmd(&[
            b"VS.SEARCH",
            b"myvec",
            b"1.0",
            b"2.0",
            b"3.0",
            b"COUNT",
            b"1",
        ])
        .await;
    match resp {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 1);
        }
        other => panic!("VS.SEARCH should return Array, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.OPTIMIZE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_optimize_removes_deleted() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0"])
        .await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v2", b"4.0", b"5.0", b"6.0"])
        .await;

    // Delete one vector
    c.cmd(&[b"VS.DEL", b"myvec", b"v1"]).await;
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(1));

    // Optimize should return the number of removed deleted nodes
    let resp = c.cmd(&[b"VS.OPTIMIZE", b"myvec"]).await;
    // The response should be an integer (number of deleted nodes removed)
    match resp {
        RespValue::Integer(_) => {}
        other => panic!("VS.OPTIMIZE should return Integer, got {other:?}"),
    }

    // Cardinality should remain the same
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(1));

    server.shutdown();
}

// ── VS.STATS ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_stats_returns_detailed_info() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[
        b"VS.RESERVE",
        b"myvec",
        b"4",
        b"COSINE",
        b"CAPACITY",
        b"5000",
        b"M",
        b"32",
    ])
    .await;

    c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0", b"4.0"])
        .await;

    let resp = c.cmd(&[b"VS.STATS", b"myvec"]).await;
    match resp {
        RespValue::Array(items) => {
            // Should be flat array of field-value pairs
            assert!(!items.is_empty());
            assert!(items.len() % 2 == 0);

            // Check that key fields exist
            let mut found_dimension = false;
            let mut found_size = false;
            let mut found_memory = false;
            for i in (0..items.len()).step_by(2) {
                if let RespValue::BulkString(field) = &items[i] {
                    let field_str = String::from_utf8_lossy(field);
                    if field_str == "dimension" {
                        found_dimension = true;
                    }
                    if field_str == "size" {
                        found_size = true;
                    }
                    if field_str == "memory_usage_bytes" {
                        found_memory = true;
                    }
                }
            }
            assert!(found_dimension, "should have dimension field");
            assert!(found_size, "should have size field");
            assert!(found_memory, "should have memory_usage_bytes field");
        }
        other => panic!("VS.STATS should return Array, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.EXPIRE / VS.TTL ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_expire_and_ttl() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;

    // No TTL set yet -> -1
    assert_eq!(c.cmd(&[b"VS.TTL", b"myvec"]).await, int(-1));

    // Set TTL
    assert_eq!(c.cmd(&[b"VS.EXPIRE", b"myvec", b"3600"]).await, int(1));

    // TTL should be ~3600
    let resp = c.cmd(&[b"VS.TTL", b"myvec"]).await;
    match resp {
        RespValue::Integer(ttl) => {
            assert!(ttl > 3500 && ttl <= 3600);
        }
        other => panic!("VS.TTL should return Integer, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.QUANTIZE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_quantize_int8() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v1", b"1.0", b"2.0", b"3.0"])
        .await;
    c.cmd(&[b"VS.ADD", b"myvec", b"v2", b"4.0", b"5.0", b"6.0"])
        .await;

    // Enable INT8 quantization - should return bytes saved
    let resp = c.cmd(&[b"VS.QUANTIZE", b"myvec", b"INT8"]).await;
    match resp {
        RespValue::Integer(saved) => {
            assert!(saved >= 0);
        }
        other => panic!("VS.QUANTIZE should return Integer, got {other:?}"),
    }

    // Search should still work after quantization
    let resp = c
        .cmd(&[
            b"VS.SEARCH",
            b"myvec",
            b"1.0",
            b"2.0",
            b"3.0",
            b"COUNT",
            b"1",
        ])
        .await;
    match resp {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 1);
        }
        other => panic!("VS.SEARCH should return Array, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.CARD with FILTER ────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_card_with_filter() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"L2"]).await;
    c.cmd(&[
        b"VS.ADD",
        b"myvec",
        b"v1",
        b"1.0",
        b"2.0",
        b"3.0",
        b"METADATA",
        br#"{"type":"a"}"#,
    ])
    .await;
    c.cmd(&[
        b"VS.ADD",
        b"myvec",
        b"v2",
        b"4.0",
        b"5.0",
        b"6.0",
        b"METADATA",
        br#"{"type":"b"}"#,
    ])
    .await;
    c.cmd(&[
        b"VS.ADD",
        b"myvec",
        b"v3",
        b"7.0",
        b"8.0",
        b"9.0",
        b"METADATA",
        br#"{"type":"a"}"#,
    ])
    .await;

    // Total count
    assert_eq!(c.cmd(&[b"VS.CARD", b"myvec"]).await, int(3));

    // Count with filter type=a
    assert_eq!(
        c.cmd(&[b"VS.CARD", b"myvec", b"FILTER", b"type=a"]).await,
        int(2)
    );

    // Count with filter type=b
    assert_eq!(
        c.cmd(&[b"VS.CARD", b"myvec", b"FILTER", b"type=b"]).await,
        int(1)
    );

    server.shutdown();
}

// ── VS.TRAINPQ ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_trainpq_basic() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // Create index and add some vectors
    assert_eq!(c.cmd(&[b"VS.RESERVE", b"myvec", b"4", b"L2"]).await, ok());

    for i in 0..20 {
        let v1 = (i as f32).to_string();
        let v2 = (i as f32 + 1.0).to_string();
        let v3 = (i as f32 + 2.0).to_string();
        let v4 = (i as f32 + 3.0).to_string();
        let id = format!("v{i}");
        c.cmd(&[
            b"VS.ADD",
            b"myvec",
            id.as_bytes(),
            v1.as_bytes(),
            v2.as_bytes(),
            v3.as_bytes(),
            v4.as_bytes(),
        ])
        .await;
    }

    // Train PQ
    let resp = c
        .cmd(&[b"VS.TRAINPQ", b"myvec", b"SUBSPACES", b"2", b"BITS", b"8"])
        .await;
    // Should return array with subspaces and bits info
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4); // subspaces key, subspaces val, bits key, bits val
        }
        other => panic!("expected array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_vs_trainpq_wrong_type() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // Create a string key
    c.cmd(&[b"SET", b"mykey", b"hello"]).await;

    let resp = c.cmd(&[b"VS.TRAINPQ", b"mykey"]).await;
    match resp {
        RespValue::Error(_) => {}
        other => panic!("expected error for wrong type, got {other:?}"),
    }

    server.shutdown();
}

// ── VS.RESERVE invalid args ────────────────────────────────────────────────

#[tokio::test]
async fn test_vs_reserve_invalid_metric() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let resp = c.cmd(&[b"VS.RESERVE", b"myvec", b"3", b"INVALID"]).await;
    match resp {
        RespValue::Error(_) => {}
        other => panic!("expected error for invalid metric, got {other:?}"),
    }

    server.shutdown();
}
