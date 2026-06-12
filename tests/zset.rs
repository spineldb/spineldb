mod common;

use spineldb::core::RespValue;

fn bs(s: &[u8]) -> RespValue {
    RespValue::BulkString(bytes::Bytes::copy_from_slice(s))
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

const NULL: RespValue = RespValue::Null;

// ── ZADD / ZRANGE / ZCARD / ZSCORE / ZREM ────────────────────────────────

#[tokio::test]
async fn test_zset_zadd_zrange() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a"]).await;
    c.cmd(&[b"ZADD", b"myzset", b"2", b"b"]).await;
    c.cmd(&[b"ZADD", b"myzset", b"3", b"c"]).await;
    let resp = c.cmd(&[b"ZRANGE", b"myzset", b"0", b"-1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"a"), bs(b"b"), bs(b"c")]);
        }
        _ => panic!("ZRANGE should return Array"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_zset_zrange_withscores() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a"]).await;
    c.cmd(&[b"ZADD", b"myzset", b"2", b"b"]).await;
    let resp = c
        .cmd(&[b"ZRANGE", b"myzset", b"0", b"-1", b"WITHSCORES"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], bs(b"a"));
            assert_eq!(items[1], bs(b"1"));
        }
        _ => panic!("ZRANGE WITHSCORES should return Array"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_zset_zscore_zrem() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"5", b"member1"]).await;
    assert_eq!(c.cmd(&[b"ZSCORE", b"myzset", b"member1"]).await, bs(b"5"));
    assert_eq!(c.cmd(&[b"ZSCORE", b"myzset", b"missing"]).await, NULL);
    assert_eq!(c.cmd(&[b"ZREM", b"myzset", b"member1"]).await, int(1));
    assert_eq!(c.cmd(&[b"ZCARD", b"myzset"]).await, int(0));
    server.shutdown();
}

// ── ZREVRANGE ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zrevrange() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    let resp = c.cmd(&[b"ZREVRANGE", b"myzset", b"0", b"-1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"c"), bs(b"b"), bs(b"a")]);
        }
        other => panic!("ZREVRANGE should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── ZRANGEBYSCORE ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zrangebyscore() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    let resp = c.cmd(&[b"ZRANGEBYSCORE", b"myzset", b"1", b"2"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"a"), bs(b"b")]);
        }
        other => panic!("ZRANGEBYSCORE should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── ZCOUNT ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zcount() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    assert_eq!(c.cmd(&[b"ZCOUNT", b"myzset", b"1", b"2"]).await, int(2));
    assert_eq!(c.cmd(&[b"ZCOUNT", b"myzset", b"(3", b"3"]).await, int(0));
    server.shutdown();
}

// ── ZRANK / ZREVRANK ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zrank() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    assert_eq!(c.cmd(&[b"ZRANK", b"myzset", b"a"]).await, int(0));
    assert_eq!(c.cmd(&[b"ZRANK", b"myzset", b"c"]).await, int(2));
    assert_eq!(c.cmd(&[b"ZRANK", b"myzset", b"z"]).await, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_zset_zrevrank() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    assert_eq!(c.cmd(&[b"ZREVRANK", b"myzset", b"c"]).await, int(0));
    assert_eq!(c.cmd(&[b"ZREVRANK", b"myzset", b"a"]).await, int(2));
    server.shutdown();
}

// ── ZINCRBY ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zincrby() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"5", b"member"]).await;
    let resp = c.cmd(&[b"ZINCRBY", b"myzset", b"3", b"member"]).await;
    match &resp {
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(b);
            assert_eq!(s, "8");
        }
        other => panic!("ZINCRBY should return BulkString, got {other:?}"),
    }
    server.shutdown();
}

// ── ZLEXCOUNT ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zlexcount() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"0", b"a", b"0", b"b", b"0", b"c"])
        .await;
    assert_eq!(
        c.cmd(&[b"ZLEXCOUNT", b"myzset", b"[a", b"[c"]).await,
        int(3)
    );
    assert_eq!(
        c.cmd(&[b"ZLEXCOUNT", b"myzset", b"(a", b"(c"]).await,
        int(1)
    );
    server.shutdown();
}

// ── ZRANGEBYLEX ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zrangebylex() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[
        b"ZADD", b"myzset", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d",
    ])
    .await;
    let resp = c.cmd(&[b"ZRANGEBYLEX", b"myzset", b"[b", b"[d"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items, vec![bs(b"b"), bs(b"c"), bs(b"d")]);
        }
        other => panic!("ZRANGEBYLEX should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── ZPOPMIN / ZPOPMAX ────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zpopmin() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    let resp = c.cmd(&[b"ZPOPMIN", b"myzset"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], bs(b"a"));
            assert_eq!(items[1], bs(b"1"));
        }
        other => panic!("ZPOPMIN should return Array, got {other:?}"),
    }
    assert_eq!(c.cmd(&[b"ZCARD", b"myzset"]).await, int(2));
    server.shutdown();
}

#[tokio::test]
async fn test_zset_zpopmax() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    let resp = c.cmd(&[b"ZPOPMAX", b"myzset"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], bs(b"c"));
            assert_eq!(items[1], bs(b"3"));
        }
        other => panic!("ZPOPMAX should return Array, got {other:?}"),
    }
    assert_eq!(c.cmd(&[b"ZCARD", b"myzset"]).await, int(2));
    server.shutdown();
}

// ── ZUNIONSTORE / ZINTERSTORE ────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zunionstore() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"z1", b"1", b"a", b"2", b"b"]).await;
    c.cmd(&[b"ZADD", b"z2", b"3", b"b", b"4", b"c"]).await;
    assert_eq!(
        c.cmd(&[b"ZUNIONSTORE", b"dest", b"2", b"z1", b"z2"]).await,
        int(3)
    );
    let resp = c
        .cmd(&[b"ZRANGE", b"dest", b"0", b"-1", b"WITHSCORES"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert!(
                items.len() >= 4,
                "should have at least 2 members with scores"
            );
        }
        other => panic!("ZRANGE should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_zset_zinterstore() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"z1", b"1", b"a", b"2", b"b"]).await;
    c.cmd(&[b"ZADD", b"z2", b"3", b"b", b"4", b"c"]).await;
    assert_eq!(
        c.cmd(&[b"ZINTERSTORE", b"dest", b"2", b"z1", b"z2"]).await,
        int(1)
    );
    assert_eq!(
        c.cmd(&[b"ZRANGE", b"dest", b"0", b"-1"]).await,
        RespValue::Array(vec![bs(b"b")])
    );
    server.shutdown();
}

// ── ZREMRANGEBYSCORE / ZREMRANGEBYRANK / ZREMRANGEBYLEX ─────────────────

#[tokio::test]
async fn test_zset_zremrangebyscore() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    assert_eq!(
        c.cmd(&[b"ZREMRANGEBYSCORE", b"myzset", b"1", b"2"]).await,
        int(2)
    );
    assert_eq!(c.cmd(&[b"ZCARD", b"myzset"]).await, int(1));
    server.shutdown();
}

#[tokio::test]
async fn test_zset_zremrangebyrank() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    assert_eq!(
        c.cmd(&[b"ZREMRANGEBYRANK", b"myzset", b"0", b"1"]).await,
        int(2)
    );
    assert_eq!(c.cmd(&[b"ZCARD", b"myzset"]).await, int(1));
    server.shutdown();
}

#[tokio::test]
async fn test_zset_zremrangebylex() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"0", b"a", b"0", b"b", b"0", b"c"])
        .await;
    assert_eq!(
        c.cmd(&[b"ZREMRANGEBYLEX", b"myzset", b"[a", b"[b"]).await,
        int(2)
    );
    assert_eq!(c.cmd(&[b"ZCARD", b"myzset"]).await, int(1));
    server.shutdown();
}

// ── ZMSCORE ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zmscore() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1.5", b"a", b"2.5", b"b"])
        .await;
    let resp = c
        .cmd(&[b"ZMSCORE", b"myzset", b"a", b"missing", b"b"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"1.5"));
            assert_eq!(items[1], NULL);
            assert_eq!(items[2], bs(b"2.5"));
        }
        other => panic!("ZMSCORE should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── ZRANGESTORE ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_zset_zrangestore() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"myzset", b"1", b"a", b"2", b"b", b"3", b"c"])
        .await;
    assert_eq!(
        c.cmd(&[b"ZRANGESTORE", b"dest", b"myzset", b"0", b"1"])
            .await,
        int(2)
    );
    assert_eq!(
        c.cmd(&[b"ZRANGE", b"dest", b"0", b"-1"]).await,
        RespValue::Array(vec![bs(b"a"), bs(b"b")])
    );
    server.shutdown();
}

// ── BZPOPMIN / BZPOPMAX (blocking - use timeout) ─────────────────────────

#[tokio::test]
async fn test_bzpopmin_with_data() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"zs", b"1.0", b"a", b"2.0", b"b"]).await;
    let resp = c.cmd(&[b"BZPOPMIN", b"zs", b"1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"zs"));
            assert_eq!(items[1], bs(b"a"));
        }
        other => panic!("BZPOPMIN should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_bzpopmin_timeout() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BZPOPMIN", b"empty", b"1"]).await;
    assert_eq!(resp, NULL);
    server.shutdown();
}

#[tokio::test]
async fn test_bzpopmax_with_data() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"ZADD", b"zs", b"1.0", b"a", b"2.0", b"b"]).await;
    let resp = c.cmd(&[b"BZPOPMAX", b"zs", b"1"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], bs(b"zs"));
            assert_eq!(items[1], bs(b"b"));
        }
        other => panic!("BZPOPMAX should return Array, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_bzpopmax_timeout() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    let resp = c.cmd(&[b"BZPOPMAX", b"empty", b"1"]).await;
    assert_eq!(resp, NULL);
    server.shutdown();
}
