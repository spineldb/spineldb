mod common;

use spineldb::core::RespValue;

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

const NULL: RespValue = RespValue::Null;

// ── GEOADD / GEOPOS ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_geo_geoadd_geopos() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    assert_eq!(
        c.cmd(&[b"GEOADD", b"cities", b"13.361389", b"38.115556", b"Palermo"])
            .await,
        int(1)
    );
    assert_eq!(
        c.cmd(&[b"GEOADD", b"cities", b"15.087269", b"37.502669", b"Catania"])
            .await,
        int(1)
    );
    let resp = c
        .cmd(&[b"GEOPOS", b"cities", b"Palermo", b"Catania", b"Missing"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            match &items[0] {
                RespValue::Array(inner) => {
                    assert_eq!(inner.len(), 2);
                }
                other => panic!("GEOPOS inner should be Array, got {other:?}"),
            }
            assert_eq!(items[2], NULL);
        }
        other => panic!("GEOPOS should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── GEODIST ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_geo_geodist() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"GEOADD", b"points", b"13.361389", b"38.115556", b"A"])
        .await;
    c.cmd(&[b"GEOADD", b"points", b"15.087269", b"37.502669", b"B"])
        .await;
    let resp = c.cmd(&[b"GEODIST", b"points", b"A", b"B", b"km"]).await;
    match &resp {
        RespValue::Double(dist) => {
            assert!(
                *dist > 50.0 && *dist < 300.0,
                "distance out of range: {dist}"
            );
        }
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(b);
            let dist: f64 = s.parse().expect("distance should be a number");
            assert!(dist > 50.0 && dist < 300.0, "distance out of range: {dist}");
        }
        other => panic!("GEODIST should return Double or BulkString, got {other:?}"),
    }
    server.shutdown();
}

// ── GEOHASH ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_geo_geohash() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"GEOADD", b"cities", b"13.361389", b"38.115556", b"Palermo"])
        .await;
    let resp = c.cmd(&[b"GEOHASH", b"cities", b"Palermo"]).await;
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                RespValue::BulkString(b) => {
                    let s = String::from_utf8_lossy(b);
                    assert!(s.len() >= 5, "geohash too short: {s}");
                }
                other => panic!("GEOHASH element should be BulkString, got {other:?}"),
            }
        }
        other => panic!("GEOHASH should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── GEORADIUS ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_geo_georadius() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"GEOADD", b"cities", b"13.361389", b"38.115556", b"Palermo"])
        .await;
    c.cmd(&[b"GEOADD", b"cities", b"15.087269", b"37.502669", b"Catania"])
        .await;
    let resp = c
        .cmd(&[
            b"GEORADIUS",
            b"cities",
            b"15",
            b"37",
            b"200",
            b"km",
            b"COUNT",
            b"10",
        ])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert!(!items.is_empty(), "GEORADIUS should find at least one city");
        }
        other => panic!("GEORADIUS should return Array, got {other:?}"),
    }
    server.shutdown();
}

// ── GEORADIUSBYMEMBER ────────────────────────────────────────────────────

#[tokio::test]
async fn test_geo_georadiusbymember() {
    let server = common::start_server().await;
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], server.port).into();
    let mut c = common::Client::connect(addr).await;
    c.cmd(&[b"GEOADD", b"cities", b"13.361389", b"38.115556", b"Palermo"])
        .await;
    c.cmd(&[b"GEOADD", b"cities", b"15.087269", b"37.502669", b"Catania"])
        .await;
    let resp = c
        .cmd(&[b"GEORADIUSBYMEMBER", b"cities", b"Palermo", b"200", b"km"])
        .await;
    match resp {
        RespValue::Array(items) => {
            assert!(!items.is_empty());
        }
        other => panic!("GEORADIUSBYMEMBER should return Array, got {other:?}"),
    }
    server.shutdown();
}
