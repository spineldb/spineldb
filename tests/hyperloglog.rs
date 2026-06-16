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

// ── PFADD edge cases ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_hll_pfadd_no_elements_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // PFADD with just a key and no elements should return 0.
    assert_eq!(c.cmd(&[b"PFADD", b"hll"]).await, int(0));
    // Key should not exist yet (no elements were added).
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(0));

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfadd_returns_zero_for_all_duplicates() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    assert_eq!(c.cmd(&[b"PFADD", b"hll", b"x", b"y", b"z"]).await, int(1));
    // All elements already exist, should return 0.
    assert_eq!(c.cmd(&[b"PFADD", b"hll", b"x", b"y"]).await, int(0));
    // PFCOUNT still reflects the 3 original elements.
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(3));

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfadd_type_mismatch() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // Set key as a string first.
    c.cmd(&[b"SET", b"hll", b"not-a-hyperloglog"]).await;
    // PFADD on a string key should return WrongType error.
    let resp = c.cmd(&[b"PFADD", b"hll", b"elem"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "PFADD on string key should return error, got {resp:?}"
    );

    server.shutdown();
}

// ── PFCOUNT edge cases ───────────────────────────────────────────────────

#[tokio::test]
async fn test_hll_pfcount_missing_key_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // PFCOUNT on a key that doesn't exist should return 0.
    assert_eq!(c.cmd(&[b"PFCOUNT", b"nonexistent"]).await, int(0));

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfcount_mixed_existing_and_missing() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"PFADD", b"hll1", b"a", b"b"]).await;
    // hll2 does not exist.
    let resp = c.cmd(&[b"PFCOUNT", b"hll1", b"hll2"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(n >= 2, "PFCOUNT should be at least 2, got {n}");
        }
        other => panic!("PFCOUNT should return Integer, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfcount_type_mismatch() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"SET", b"strkey", b"not-hll"]).await;
    let resp = c.cmd(&[b"PFCOUNT", b"strkey"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "PFCOUNT on string key should return error, got {resp:?}"
    );

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfcount_no_args_returns_zero() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let resp = c.cmd(&[b"PFCOUNT"]).await;
    match resp {
        RespValue::Error(_) => {} // expected
        other => panic!("PFCOUNT with no args should return error, got {other:?}"),
    }

    server.shutdown();
}

// ── PFMERGE edge cases ───────────────────────────────────────────────────

#[tokio::test]
async fn test_hll_pfmerge_nonexistent_source_keys() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // PFMERGE with source keys that don't exist should create an empty HLL.
    assert_eq!(
        c.cmd(&[b"PFMERGE", b"dest", b"no1", b"no2"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(c.cmd(&[b"PFCOUNT", b"dest"]).await, int(0));

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfmerge_dest_overwrites_previous() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // Populate dest with its own data.
    c.cmd(&[b"PFADD", b"dest", b"old1", b"old2"]).await;
    assert_eq!(c.cmd(&[b"PFCOUNT", b"dest"]).await, int(2));

    // Create sources with different data.
    c.cmd(&[b"PFADD", b"src1", b"new1"]).await;
    c.cmd(&[b"PFADD", b"src2", b"new2"]).await;

    // PFMERGE should overwrite dest entirely.
    assert_eq!(
        c.cmd(&[b"PFMERGE", b"dest", b"src1", b"src2"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    let resp = c.cmd(&[b"PFCOUNT", b"dest"]).await;
    match resp {
        RespValue::Integer(n) => {
            assert!(n >= 2, "dest should now have at least 2, got {n}");
        }
        other => panic!("PFCOUNT should return Integer, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfmerge_overwrites_string_key() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"SET", b"dest", b"string-value"]).await;
    c.cmd(&[b"PFADD", b"src", b"a"]).await;
    // PFMERGE overwrites the dest key, even if it was a string (same as Redis).
    assert_eq!(
        c.cmd(&[b"PFMERGE", b"dest", b"src"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    // dest is now an HLL with 1 element.
    assert_eq!(c.cmd(&[b"PFCOUNT", b"dest"]).await, int(1));

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfmerge_type_mismatch_on_source() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"SET", b"src", b"string-value"]).await;
    let resp = c.cmd(&[b"PFMERGE", b"dest", b"src"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "PFMERGE with a string source should return error, got {resp:?}"
    );

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfmerge_no_args_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let resp = c.cmd(&[b"PFMERGE"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "PFMERGE with no args should return error, got {resp:?}"
    );

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfmerge_single_arg_returns_error() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // PFMERGE with only dest key (no source) should be an error.
    let resp = c.cmd(&[b"PFMERGE", b"dest"]).await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "PFMERGE with only dest should return error, got {resp:?}"
    );

    server.shutdown();
}

// ── Accuracy & stress tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_hll_accuracy_large_cardinality() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // Add 10,000 distinct elements.
    let mut args: Vec<&[u8]> = Vec::new();
    let keys: Vec<String> = (0..10_000).map(|i| format!("item-{i}")).collect();
    // PFADD can take multiple elements at once; batch them.
    for chunk in keys.chunks(100) {
        args.clear();
        args.push(b"PFADD");
        args.push(b"hll-big");
        for k in chunk {
            args.push(k.as_bytes());
        }
        c.cmd(&args).await;
    }

    let resp = c.cmd(&[b"PFCOUNT", b"hll-big"]).await;
    match resp {
        RespValue::Integer(n) => {
            let error = (n as f64 - 10_000.0).abs() / 10_000.0;
            assert!(
                error < 0.05,
                "estimate {n} has error {error:.4} (>5%) for 10k items"
            );
        }
        other => panic!("PFCOUNT should return Integer, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_hll_merge_accuracy() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // Add 5000 elements to hll_a and 5000 different elements to hll_b.
    let mut args: Vec<&[u8]> = Vec::new();
    let keys_a: Vec<String> = (0..5_000).map(|i| format!("a-{i}")).collect();
    for chunk in keys_a.chunks(100) {
        args.clear();
        args.push(b"PFADD");
        args.push(b"hll_a");
        for k in chunk {
            args.push(k.as_bytes());
        }
        c.cmd(&args).await;
    }
    let keys_b: Vec<String> = (0..5_000).map(|i| format!("b-{i}")).collect();
    for chunk in keys_b.chunks(100) {
        args.clear();
        args.push(b"PFADD");
        args.push(b"hll_b");
        for k in chunk {
            args.push(k.as_bytes());
        }
        c.cmd(&args).await;
    }

    assert_eq!(
        c.cmd(&[b"PFMERGE", b"hll_merged", b"hll_a", b"hll_b"])
            .await,
        RespValue::SimpleString("OK".to_string())
    );

    let resp = c.cmd(&[b"PFCOUNT", b"hll_merged"]).await;
    match resp {
        RespValue::Integer(n) => {
            let error = (n as f64 - 10_000.0).abs() / 10_000.0;
            assert!(
                error < 0.05,
                "merged estimate {n} has error {error:.4} (>5%) for 10k items"
            );
        }
        other => panic!("PFCOUNT should return Integer, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_hll_pfmerge_into_self() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"PFADD", b"hll", b"a", b"b", b"c"]).await;
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(3));

    // Merging a key into itself should preserve the count.
    assert_eq!(
        c.cmd(&[b"PFMERGE", b"hll", b"hll"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(3));

    server.shutdown();
}

#[tokio::test]
async fn test_hll_many_duplicates_stable_count() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"PFADD", b"hll", b"a", b"b", b"c"]).await;
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(3));

    // Add the same elements many more times.
    for _ in 0..100 {
        c.cmd(&[b"PFADD", b"hll", b"a", b"b", b"c"]).await;
    }
    assert_eq!(c.cmd(&[b"PFCOUNT", b"hll"]).await, int(3));

    server.shutdown();
}
