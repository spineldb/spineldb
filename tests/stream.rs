mod common;

use spineldb::core::RespValue;

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

// ── XADD / XLEN / XRANGE ────────────────────────────────────────────────

#[tokio::test]
async fn test_stream_xadd_xlen_xrange() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let id1 = match c
        .cmd(&[b"XADD", b"mystream", b"*", b"name", b"alice"])
        .await
    {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("XADD should return BulkString, got {other:?}"),
    };
    assert!(!id1.is_empty(), "XADD should return a non-empty ID");

    let id2 = match c.cmd(&[b"XADD", b"mystream", b"*", b"name", b"bob"]).await {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("XADD should return BulkString, got {other:?}"),
    };

    assert_eq!(c.cmd(&[b"XLEN", b"mystream"]).await, int(2));

    let resp = c.cmd(&[b"XRANGE", b"mystream", b"-", b"+"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        other => panic!("XRANGE should return Array, got {other:?}"),
    }

    let _ = id1;
    let _ = id2;
    server.shutdown();
}

// ── XDEL / XTRIM ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_stream_xdel() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    let id1 = match c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v1"]).await {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("XADD should return BulkString, got {other:?}"),
    };
    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v2"]).await;

    assert_eq!(c.cmd(&[b"XDEL", b"mystream", id1.as_bytes()]).await, int(1));
    assert_eq!(c.cmd(&[b"XLEN", b"mystream"]).await, int(1));
    server.shutdown();
}

#[tokio::test]
async fn test_stream_xtrim() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    for _ in 0..10 {
        c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    }
    assert_eq!(c.cmd(&[b"XLEN", b"mystream"]).await, int(10));

    let trimmed = match c.cmd(&[b"XTRIM", b"mystream", b"MAXLEN", b"5"]).await {
        RespValue::Integer(n) => n,
        other => panic!("XTRIM should return Integer, got {other:?}"),
    };
    assert!(trimmed >= 0);
    server.shutdown();
}

// ── XREVRANGE ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_stream_xrevrange() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"n", b"1"]).await;
    c.cmd(&[b"XADD", b"mystream", b"*", b"n", b"2"]).await;

    let resp = c.cmd(&[b"XREVRANGE", b"mystream", b"+", b"-"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
        }
        other => panic!("XREVRANGE should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── XINFO ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_stream_xinfo_stream() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    let resp = c.cmd(&[b"XINFO", b"STREAM", b"mystream"]).await;
    match resp {
        RespValue::Array(items) => {
            assert!(
                !items.is_empty(),
                "XINFO STREAM should return non-empty Array"
            );
        }
        other => panic!("XINFO STREAM should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── XGROUP CREATE / XREADGROUP / XACK ─────────────────────────────────────

#[tokio::test]
async fn test_stream_xgroup_create_and_destroy() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;

    // Create consumer group
    assert_eq!(
        c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"0"])
            .await,
        RespValue::SimpleString("OK".into())
    );

    // Duplicate create should error
    let resp = c
        .cmd(&[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"0"])
        .await;
    assert!(
        matches!(resp, RespValue::Error(_)),
        "duplicate XGROUP CREATE should error"
    );

    // Destroy consumer group
    assert_eq!(
        c.cmd(&[b"XGROUP", b"DESTROY", b"mystream", b"mygroup"])
            .await,
        int(1)
    );

    // Destroy non-existent group returns 0
    assert_eq!(
        c.cmd(&[b"XGROUP", b"DESTROY", b"mystream", b"mygroup"])
            .await,
        int(0)
    );

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xgroup_create_mkstream() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // Create group on non-existing stream with MKSTREAM
    assert_eq!(
        c.cmd(&[
            b"XGROUP",
            b"CREATE",
            b"newstream",
            b"grp",
            b"$",
            b"MKSTREAM"
        ])
        .await,
        RespValue::SimpleString("OK".into())
    );

    // Stream should exist now (but empty)
    assert_eq!(c.cmd(&[b"XLEN", b"newstream"]).await, int(0));

    // Without MKSTREAM on non-existing stream should error
    let resp = c
        .cmd(&[b"XGROUP", b"CREATE", b"noexist", b"grp2", b"$"])
        .await;
    assert!(matches!(resp, RespValue::Error(_)));

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xreadgroup_new_entries() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"name", b"alice"])
        .await;
    c.cmd(&[b"XADD", b"mystream", b"*", b"name", b"bob"]).await;

    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"0"])
        .await;

    // Read new entries with >
    let resp = c
        .cmd(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"consumer1",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .await;

    match resp {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1, "should return data for 1 stream");
        }
        other => panic!("XREADGROUP should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xack() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v1"]).await;
    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v2"]).await;

    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // Read new entries
    let _resp = c
        .cmd(&[
            b"XREADGROUP",
            b"GROUP",
            b"grp",
            b"consumer1",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .await;

    // Get the IDs to acknowledge
    let pending = match c
        .cmd(&[b"XPENDING", b"mystream", b"grp", b"-", b"+", b"10"])
        .await
    {
        RespValue::Array(items) => items,
        other => panic!("XPENDING should return Array, got {other:?}"),
    };

    assert_eq!(pending.len(), 2, "should have 2 pending entries");

    // Extract ID from first pending entry
    let id1 = match &pending[0] {
        RespValue::Array(entry) => match &entry[0] {
            RespValue::BulkString(id) => id.clone(),
            other => panic!("expected BulkString ID, got {other:?}"),
        },
        other => panic!("expected Array, got {other:?}"),
    };

    // Acknowledge first entry
    assert_eq!(c.cmd(&[b"XACK", b"mystream", b"grp", &id1]).await, int(1));

    // Acknowledge non-existent ID returns 0
    assert_eq!(
        c.cmd(&[b"XACK", b"mystream", b"grp", b"999-0"]).await,
        int(0)
    );

    // Pending count should now be 1
    let pending2 = match c
        .cmd(&[b"XPENDING", b"mystream", b"grp", b"-", b"+", b"10"])
        .await
    {
        RespValue::Array(items) => items,
        other => panic!("XPENDING should return Array, got {other:?}"),
    };
    assert_eq!(pending2.len(), 1);

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xpending_summary() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // No pending entries yet
    let resp = c.cmd(&[b"XPENDING", b"mystream", b"grp"]).await;
    match resp {
        RespValue::Array(summary) => {
            assert_eq!(summary.len(), 4, "summary should have 4 elements");
            // pending count = 0
            assert_eq!(summary[0], int(0));
        }
        other => panic!("XPENDING summary should return Array, got {other:?}"),
    }

    // Add entries and read them
    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v2"]).await;
    let _ = c
        .cmd(&[
            b"XREADGROUP",
            b"GROUP",
            b"grp",
            b"c1",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .await;

    // Now should have pending entries
    let resp2 = c.cmd(&[b"XPENDING", b"mystream", b"grp"]).await;
    match resp2 {
        RespValue::Array(summary) => {
            assert_eq!(summary[0], int(2), "should have 2 pending entries");
        }
        other => panic!("XPENDING summary should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xclaim() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // Read entries as consumer1
    let _ = c
        .cmd(&[
            b"XREADGROUP",
            b"GROUP",
            b"grp",
            b"consumer1",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .await;

    // Get pending IDs
    let pending = match c
        .cmd(&[b"XPENDING", b"mystream", b"grp", b"-", b"+", b"10"])
        .await
    {
        RespValue::Array(items) => items,
        other => panic!("XPENDING should return Array, got {other:?}"),
    };
    assert_eq!(pending.len(), 1);

    let id = match &pending[0] {
        RespValue::Array(entry) => match &entry[0] {
            RespValue::BulkString(id) => String::from_utf8_lossy(id).into_owned(),
            other => panic!("expected BulkString, got {other:?}"),
        },
        other => panic!("expected Array, got {other:?}"),
    };

    // Claim with min_idle_time=0 (immediate) and JUSTID
    let resp = c
        .cmd(&[
            b"XCLAIM",
            b"mystream",
            b"grp",
            b"consumer2",
            b"0",
            b"JUSTID",
            id.as_bytes(),
        ])
        .await;

    match resp {
        RespValue::Array(ids) => {
            assert_eq!(ids.len(), 1, "should have claimed 1 entry");
        }
        other => panic!("XCLAIM should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xgroup_setid() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // Set group ID to 0-0
    assert_eq!(
        c.cmd(&[b"XGROUP", b"SETID", b"mystream", b"grp", b"0-0"])
            .await,
        RespValue::SimpleString("OK".into())
    );

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xread_basic() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v1"]).await;
    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v2"]).await;

    // Read all entries from ID 0-0
    let resp = c.cmd(&[b"XREAD", b"STREAMS", b"mystream", b"0-0"]).await;

    match resp {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1, "should return data for 1 stream");
        }
        other => panic!("XREAD should return Array, got {other:?}"),
    }

    // Read from last entry - should return null
    let resp2 = c.cmd(&[b"XREAD", b"STREAMS", b"mystream", b"+"]).await;
    assert_eq!(resp2, RespValue::Null);

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xdel_consumer_group() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // Delete consumer that doesn't exist returns 0
    assert_eq!(
        c.cmd(&[b"XGROUP", b"DELCONSUMER", b"mystream", b"grp", b"ghost"])
            .await,
        int(0)
    );

    server.shutdown();
}

// ── XAUTOCLAIM ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_stream_xautoclaim_basic() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v1"]).await;
    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v2"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // Read entries as consumer1 (they become pending)
    let _ = c
        .cmd(&[
            b"XREADGROUP",
            b"GROUP",
            b"grp",
            b"consumer1",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .await;

    // Auto-claim with min_idle_time=0 for consumer2
    let resp = c
        .cmd(&[
            b"XAUTOCLAIM",
            b"mystream",
            b"grp",
            b"consumer2",
            b"0",
            b"0-0",
        ])
        .await;

    match resp {
        RespValue::Array(result) => {
            assert_eq!(
                result.len(),
                2,
                "XAUTOCLAIM should return [next_start_id, entries]"
            );
            // First element is the next start ID (a BulkString)
            match &result[0] {
                RespValue::BulkString(_) => {}
                other => panic!("next_start_id should be BulkString, got {other:?}"),
            }
            // Second element is the entries array
            match &result[1] {
                RespValue::Array(entries) => {
                    assert_eq!(entries.len(), 2, "should have claimed 2 entries");
                }
                other => panic!("entries should be Array, got {other:?}"),
            }
        }
        other => panic!("XAUTOCLAIM should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xautoclaim_justid() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    let _ = c
        .cmd(&[
            b"XREADGROUP",
            b"GROUP",
            b"grp",
            b"c1",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .await;

    // Auto-claim with JUSTID
    let resp = c
        .cmd(&[
            b"XAUTOCLAIM",
            b"mystream",
            b"grp",
            b"c2",
            b"0",
            b"0-0",
            b"JUSTID",
        ])
        .await;

    match resp {
        RespValue::Array(result) => {
            assert_eq!(result.len(), 2);
            // With JUSTID, second element should be an array of ID strings
            match &result[1] {
                RespValue::Array(ids) => {
                    assert_eq!(ids.len(), 1, "should have claimed 1 entry ID");
                }
                other => panic!("ids should be Array, got {other:?}"),
            }
        }
        other => panic!("XAUTOCLAIM should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xautoclaim_no_claimable_entries() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // Auto-claim with very high min_idle_time - nothing should be claimable
    let resp = c
        .cmd(&[
            b"XAUTOCLAIM",
            b"mystream",
            b"grp",
            b"c2",
            b"999999999",
            b"0-0",
        ])
        .await;

    match resp {
        RespValue::Array(result) => {
            assert_eq!(result.len(), 2);
            match &result[1] {
                RespValue::Array(entries) => {
                    assert_eq!(entries.len(), 0, "should have claimed 0 entries");
                }
                other => panic!("entries should be Array, got {other:?}"),
            }
        }
        other => panic!("XAUTOCLAIM should return Array, got {other:?}"),
    }

    server.shutdown();
}

// ── XINFO GROUPS ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_stream_xinfo_groups() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp1", b"0"])
        .await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp2", b"0"])
        .await;

    let resp = c.cmd(&[b"XINFO", b"GROUPS", b"mystream"]).await;
    match resp {
        RespValue::Array(groups) => {
            assert_eq!(groups.len(), 2, "should have 2 groups");
            // Each group is an array of [name, consumers, pending, last-delivered-id, ...]
            for group in &groups {
                match group {
                    RespValue::Array(info) => {
                        assert!(
                            info.len() >= 4,
                            "group info should have at least 4 elements, got {}",
                            info.len()
                        );
                    }
                    other => panic!("group info should be Array, got {other:?}"),
                }
            }
        }
        other => panic!("XINFO GROUPS should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xinfo_groups_empty_stream() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    // XINFO GROUPS on a stream with no groups
    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    let resp = c.cmd(&[b"XINFO", b"GROUPS", b"mystream"]).await;
    match resp {
        RespValue::Array(groups) => {
            assert_eq!(groups.len(), 0, "should have 0 groups");
        }
        other => panic!("XINFO GROUPS should return Array, got {other:?}"),
    }

    server.shutdown();
}

// ── XINFO CONSUMERS ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_stream_xinfo_consumers() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    // Create a consumer by reading
    let _ = c
        .cmd(&[
            b"XREADGROUP",
            b"GROUP",
            b"grp",
            b"consumer1",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .await;

    let resp = c.cmd(&[b"XINFO", b"CONSUMERS", b"mystream", b"grp"]).await;
    match resp {
        RespValue::Array(consumers) => {
            assert_eq!(consumers.len(), 1, "should have 1 consumer");
            match &consumers[0] {
                RespValue::Array(info) => {
                    // Each consumer is [name, pending, idle, ...]
                    assert!(
                        info.len() >= 3,
                        "consumer info should have at least 3 elements, got {}",
                        info.len()
                    );
                }
                other => panic!("consumer info should be Array, got {other:?}"),
            }
        }
        other => panic!("XINFO CONSUMERS should return Array, got {other:?}"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_stream_xinfo_consumers_no_consumers() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;

    c.cmd(&[b"XADD", b"mystream", b"*", b"k", b"v"]).await;
    c.cmd(&[b"XGROUP", b"CREATE", b"mystream", b"grp", b"0"])
        .await;

    let resp = c.cmd(&[b"XINFO", b"CONSUMERS", b"mystream", b"grp"]).await;
    match resp {
        RespValue::Array(consumers) => {
            assert_eq!(consumers.len(), 0, "should have 0 consumers");
        }
        other => panic!("XINFO CONSUMERS should return Array, got {other:?}"),
    }

    server.shutdown();
}
