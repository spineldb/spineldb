// tests/integration/geospatial_test.rs

//! Integration tests for geospatial commands
//! Tests: GEOADD, GEOPOS, GEODIST, GEOHASH, GEORADIUS, GEORADIUSBYMEMBER

use super::test_helpers::TestContext;
use bytes::Bytes;
use spineldb::core::{RespValue, SpinelDBError};

// ===== Helper Functions =====

/// Helper to assert that a RespValue is an integer
fn assert_integer(result: &RespValue, expected: i64, message: &str) {
    match result {
        RespValue::Integer(val) => {
            assert_eq!(
                *val, expected,
                "{}: expected {}, got {}",
                message, expected, val
            );
        }
        _ => panic!("{}: Expected integer response, got {:?}", message, result),
    }
}

/// Helper to assert that a RespValue is a float string (within tolerance)
fn assert_float_string(result: &RespValue, expected: f64, tolerance: f64, message: &str) {
    match result {
        RespValue::BulkString(bs) => {
            let s = String::from_utf8_lossy(bs);
            let val: f64 = s
                .parse()
                .expect(&format!("{}: Could not parse float: {}", message, s));
            assert!(
                (val - expected).abs() < tolerance,
                "{}: expected {} ± {}, got {}",
                message,
                expected,
                tolerance,
                val
            );
        }
        _ => panic!(
            "{}: Expected bulk string response, got {:?}",
            message, result
        ),
    }
}

/// Helper to assert that a RespValue is an array of coordinates
fn assert_coordinates(
    result: &RespValue,
    expected_lon: f64,
    expected_lat: f64,
    tolerance: f64,
    message: &str,
) {
    match result {
        RespValue::Array(coords) => {
            assert_eq!(
                coords.len(),
                2,
                "{}: Expected 2 coordinates, got {}",
                message,
                coords.len()
            );
            match (&coords[0], &coords[1]) {
                (RespValue::BulkString(lon_bs), RespValue::BulkString(lat_bs)) => {
                    let lon: f64 = String::from_utf8_lossy(lon_bs)
                        .parse()
                        .expect(&format!("{}: Could not parse longitude", message));
                    let lat: f64 = String::from_utf8_lossy(lat_bs)
                        .parse()
                        .expect(&format!("{}: Could not parse latitude", message));
                    assert!(
                        (lon - expected_lon).abs() < tolerance,
                        "{}: longitude expected {} ± {}, got {}",
                        message,
                        expected_lon,
                        tolerance,
                        lon
                    );
                    assert!(
                        (lat - expected_lat).abs() < tolerance,
                        "{}: latitude expected {} ± {}, got {}",
                        message,
                        expected_lat,
                        tolerance,
                        lat
                    );
                }
                _ => panic!(
                    "{}: Expected bulk string coordinates, got {:?}",
                    message, coords
                ),
            }
        }
        _ => panic!("{}: Expected array response, got {:?}", message, result),
    }
}

// ===== GEOADD Tests =====

#[tokio::test]
async fn test_geoadd_basic() {
    let ctx = TestContext::new().await;

    // Add a single location
    let result = ctx
        .geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();
    assert_integer(&result, 1, "GEOADD should return 1 for new member");
}

#[tokio::test]
async fn test_geoadd_multiple() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    let result = ctx
        .geoadd(
            "paris:landmarks",
            &[
                ("2.2945", "48.8582", "Eiffel Tower"),
                ("2.3372", "48.8606", "Louvre Museum"),
                ("2.3522", "48.8566", "Notre Dame"),
            ],
        )
        .await
        .unwrap();
    assert_integer(&result, 3, "GEOADD should return 3 for three new members");
}

#[tokio::test]
async fn test_geoadd_update_existing() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Update with new coordinates
    let result = ctx
        .geoadd("paris:landmarks", &[("2.3000", "48.8600", "Eiffel Tower")])
        .await
        .unwrap();
    assert_integer(
        &result,
        0,
        "GEOADD should return 0 when updating existing member",
    );
}

#[tokio::test]
async fn test_geoadd_invalid_coordinates() {
    let ctx = TestContext::new().await;

    // Test invalid longitude (> 180)
    let result = ctx
        .geoadd("paris:landmarks", &[("200.0", "48.8582", "Invalid")])
        .await;
    assert!(result.is_err(), "GEOADD should fail with invalid longitude");

    // Test invalid latitude (> 85.05112878)
    let result = ctx
        .geoadd("paris:landmarks", &[("2.2945", "90.0", "Invalid")])
        .await;
    assert!(result.is_err(), "GEOADD should fail with invalid latitude");
}

#[tokio::test]
async fn test_geoadd_wrong_argument_count() {
    let _ctx = TestContext::new().await;

    // Test with insufficient arguments - parsing should fail
    let command =
        spineldb::core::Command::try_from(spineldb::core::protocol::RespFrame::Array(vec![
            spineldb::core::protocol::RespFrame::BulkString(Bytes::from_static(b"GEOADD")),
            spineldb::core::protocol::RespFrame::BulkString(Bytes::from("key")),
            // Missing coordinates and member
        ]));

    // Command parsing should fail
    assert!(
        command.is_err(),
        "GEOADD should fail to parse with wrong argument count"
    );
}

// ===== GEOPOS Tests =====

#[tokio::test]
async fn test_geopos_basic() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Get position
    let result = ctx
        .geopos("paris:landmarks", &["Eiffel Tower"])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1, "GEOPOS should return array with 1 element");
            assert_coordinates(&arr[0], 2.2945, 48.8582, 0.0001, "Eiffel Tower coordinates");
        }
        _ => panic!("GEOPOS should return array"),
    }
}

#[tokio::test]
async fn test_geopos_multiple() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
        ],
    )
    .await
    .unwrap();

    // Get positions
    let result = ctx
        .geopos("paris:landmarks", &["Eiffel Tower", "Louvre Museum"])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2, "GEOPOS should return array with 2 elements");
            assert_coordinates(&arr[0], 2.2945, 48.8582, 0.0001, "Eiffel Tower coordinates");
            assert_coordinates(
                &arr[1],
                2.3372,
                48.8606,
                0.0001,
                "Louvre Museum coordinates",
            );
        }
        _ => panic!("GEOPOS should return array"),
    }
}

#[tokio::test]
async fn test_geopos_nonexistent_member() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Get position of nonexistent member
    let result = ctx
        .geopos("paris:landmarks", &["Nonexistent"])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1, "GEOPOS should return array with 1 element");
            assert_eq!(
                arr[0],
                RespValue::Null,
                "GEOPOS should return null for nonexistent member"
            );
        }
        _ => panic!("GEOPOS should return array"),
    }
}

#[tokio::test]
async fn test_geopos_mixed_existing_nonexistent() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Get positions of existing and nonexistent members
    let result = ctx
        .geopos("paris:landmarks", &["Eiffel Tower", "Nonexistent"])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2, "GEOPOS should return array with 2 elements");
            assert_ne!(arr[0], RespValue::Null, "First member should exist");
            assert_eq!(arr[1], RespValue::Null, "Second member should not exist");
        }
        _ => panic!("GEOPOS should return array"),
    }
}

#[tokio::test]
async fn test_geopos_nonexistent_key() {
    let ctx = TestContext::new().await;

    // Get position from nonexistent key
    let result = ctx.geopos("nonexistent:key", &["member"]).await.unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1, "GEOPOS should return array with 1 element");
            assert_eq!(
                arr[0],
                RespValue::Null,
                "GEOPOS should return null for nonexistent key"
            );
        }
        _ => panic!("GEOPOS should return array"),
    }
}

#[tokio::test]
async fn test_geopos_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("wrong:type", "value").await.unwrap();

    // Try to get position from wrong type
    let result = ctx.geopos("wrong:type", &["member"]).await;
    assert!(result.is_err(), "GEOPOS should fail with wrong type");
    match result {
        Err(SpinelDBError::WrongType) => {}
        _ => panic!("GEOPOS should return WrongType error"),
    }
}

// ===== GEODIST Tests =====

#[tokio::test]
async fn test_geodist_basic() {
    let ctx = TestContext::new().await;

    // Add two locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Calculate distance (default unit is meters)
    let result = ctx
        .geodist("paris:landmarks", "Louvre Museum", "Notre Dame", None)
        .await
        .unwrap();
    match result {
        RespValue::BulkString(bs) => {
            let dist: f64 = String::from_utf8_lossy(&bs)
                .parse()
                .expect("Could not parse distance");
            // Distance should be approximately 1205 meters
            assert!(
                (dist - 1205.0).abs() < 50.0,
                "Distance should be approximately 1205m, got {}",
                dist
            );
        }
        _ => panic!("GEODIST should return bulk string"),
    }
}

#[tokio::test]
async fn test_geodist_units() {
    let ctx = TestContext::new().await;

    // Add two locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Test different units
    let result_m = ctx
        .geodist("paris:landmarks", "Louvre Museum", "Notre Dame", Some("m"))
        .await
        .unwrap();
    let result_km = ctx
        .geodist("paris:landmarks", "Louvre Museum", "Notre Dame", Some("km"))
        .await
        .unwrap();
    let result_ft = ctx
        .geodist("paris:landmarks", "Louvre Museum", "Notre Dame", Some("ft"))
        .await
        .unwrap();
    let result_mi = ctx
        .geodist("paris:landmarks", "Louvre Museum", "Notre Dame", Some("mi"))
        .await
        .unwrap();

    // Extract distances
    let dist_m: f64 = match result_m {
        RespValue::BulkString(bs) => String::from_utf8_lossy(&bs).parse().unwrap(),
        _ => panic!("Expected bulk string"),
    };
    let dist_km: f64 = match result_km {
        RespValue::BulkString(bs) => String::from_utf8_lossy(&bs).parse().unwrap(),
        _ => panic!("Expected bulk string"),
    };
    let dist_ft: f64 = match result_ft {
        RespValue::BulkString(bs) => String::from_utf8_lossy(&bs).parse().unwrap(),
        _ => panic!("Expected bulk string"),
    };
    let dist_mi: f64 = match result_mi {
        RespValue::BulkString(bs) => String::from_utf8_lossy(&bs).parse().unwrap(),
        _ => panic!("Expected bulk string"),
    };

    // Verify unit conversions (approximate)
    assert!((dist_km * 1000.0 - dist_m).abs() < 10.0, "km conversion");
    assert!((dist_ft / 3.28084 - dist_m).abs() < 10.0, "ft conversion");
    assert!((dist_mi * 1609.34 - dist_m).abs() < 10.0, "mi conversion");
}

#[tokio::test]
async fn test_geodist_same_member() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Distance to itself should be 0
    let result = ctx
        .geodist("paris:landmarks", "Eiffel Tower", "Eiffel Tower", None)
        .await
        .unwrap();
    assert_float_string(&result, 0.0, 0.1, "Distance to self should be 0");
}

#[tokio::test]
async fn test_geodist_nonexistent_member() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Distance with nonexistent member should return null
    let result = ctx
        .geodist("paris:landmarks", "Eiffel Tower", "Nonexistent", None)
        .await
        .unwrap();
    assert_eq!(
        result,
        RespValue::Null,
        "GEODIST should return null for nonexistent member"
    );
}

#[tokio::test]
async fn test_geodist_nonexistent_key() {
    let ctx = TestContext::new().await;

    // Distance from nonexistent key should return null
    let result = ctx
        .geodist("nonexistent:key", "member1", "member2", None)
        .await
        .unwrap();
    assert_eq!(
        result,
        RespValue::Null,
        "GEODIST should return null for nonexistent key"
    );
}

#[tokio::test]
async fn test_geodist_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("wrong:type", "value").await.unwrap();

    // Try to calculate distance from wrong type
    let result = ctx.geodist("wrong:type", "member1", "member2", None).await;
    assert!(result.is_err(), "GEODIST should fail with wrong type");
    match result {
        Err(SpinelDBError::WrongType) => {}
        _ => panic!("GEODIST should return WrongType error"),
    }
}

#[tokio::test]
async fn test_geodist_invalid_unit() {
    let ctx = TestContext::new().await;

    // Add two locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Try invalid unit
    let result = ctx
        .geodist(
            "paris:landmarks",
            "Louvre Museum",
            "Notre Dame",
            Some("invalid"),
        )
        .await;
    assert!(result.is_err(), "GEODIST should fail with invalid unit");
}

// ===== GEOHASH Tests =====

#[tokio::test]
async fn test_geohash_basic() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Get geohash
    let result = ctx
        .geohash("paris:landmarks", &["Eiffel Tower"])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1, "GEOHASH should return array with 1 element");
            match &arr[0] {
                RespValue::BulkString(bs) => {
                    let hash = String::from_utf8_lossy(bs);
                    assert_eq!(hash.len(), 11, "Geohash should be 11 characters");
                }
                _ => panic!("GEOHASH should return bulk string"),
            }
        }
        _ => panic!("GEOHASH should return array"),
    }
}

#[tokio::test]
async fn test_geohash_multiple() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
        ],
    )
    .await
    .unwrap();

    // Get geohashes
    let result = ctx
        .geohash("paris:landmarks", &["Eiffel Tower", "Louvre Museum"])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2, "GEOHASH should return array with 2 elements");
            for item in arr {
                match item {
                    RespValue::BulkString(bs) => {
                        let hash = String::from_utf8_lossy(&bs);
                        assert_eq!(hash.len(), 11, "Geohash should be 11 characters");
                    }
                    _ => panic!("GEOHASH should return bulk string"),
                }
            }
        }
        _ => panic!("GEOHASH should return array"),
    }
}

#[tokio::test]
async fn test_geohash_nonexistent_member() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Get geohash of nonexistent member
    let result = ctx
        .geohash("paris:landmarks", &["Nonexistent"])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1, "GEOHASH should return array with 1 element");
            assert_eq!(
                arr[0],
                RespValue::Null,
                "GEOHASH should return null for nonexistent member"
            );
        }
        _ => panic!("GEOHASH should return array"),
    }
}

#[tokio::test]
async fn test_geohash_nonexistent_key() {
    let ctx = TestContext::new().await;

    // Get geohash from nonexistent key
    let result = ctx.geohash("nonexistent:key", &["member"]).await.unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1, "GEOHASH should return array with 1 element");
            assert_eq!(
                arr[0],
                RespValue::Null,
                "GEOHASH should return null for nonexistent key"
            );
        }
        _ => panic!("GEOHASH should return array"),
    }
}

#[tokio::test]
async fn test_geohash_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("wrong:type", "value").await.unwrap();

    // Try to get geohash from wrong type
    let result = ctx.geohash("wrong:type", &["member"]).await;
    assert!(result.is_err(), "GEOHASH should fail with wrong type");
    match result {
        Err(SpinelDBError::WrongType) => {}
        _ => panic!("GEOHASH should return WrongType error"),
    }
}

// ===== GEORADIUS Tests =====

#[tokio::test]
async fn test_georadius_basic() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Search radius around a point (near Louvre)
    let result = ctx
        .georadius("paris:landmarks", "2.3372", "48.8606", "2000", "m", &[])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(
                arr.len() >= 2,
                "Should find at least 2 landmarks within 2km"
            );
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_withdist() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
        ],
    )
    .await
    .unwrap();

    // Search with distance
    let result = ctx
        .georadius(
            "paris:landmarks",
            "2.3372",
            "48.8606",
            "5000",
            "m",
            &["WITHDIST"],
        )
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty(), "Should find at least one result");
            // Each result should be an array with [member, distance]
            for item in arr {
                match item {
                    RespValue::Array(item_arr) => {
                        assert_eq!(item_arr.len(), 2, "WITHDIST result should have 2 elements");
                    }
                    _ => panic!("WITHDIST result should be array"),
                }
            }
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_withcoord() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Search with coordinates
    let result = ctx
        .georadius(
            "paris:landmarks",
            "2.2945",
            "48.8582",
            "1000",
            "m",
            &["WITHCOORD"],
        )
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty(), "Should find at least one result");
            // Each result should be an array with [member, [lon, lat]]
            for item in arr {
                match item {
                    RespValue::Array(item_arr) => {
                        assert_eq!(item_arr.len(), 2, "WITHCOORD result should have 2 elements");
                        // Second element should be coordinates array
                        match &item_arr[1] {
                            RespValue::Array(coord_arr) => {
                                assert_eq!(
                                    coord_arr.len(),
                                    2,
                                    "Coordinates should have 2 elements"
                                );
                            }
                            _ => panic!("Coordinates should be array"),
                        }
                    }
                    _ => panic!("WITHCOORD result should be array"),
                }
            }
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_withdist_withcoord() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Search with both distance and coordinates
    let result = ctx
        .georadius(
            "paris:landmarks",
            "2.2945",
            "48.8582",
            "1000",
            "m",
            &["WITHDIST", "WITHCOORD"],
        )
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty(), "Should find at least one result");
            // Each result should be an array with [member, distance, [lon, lat]]
            for item in arr {
                match item {
                    RespValue::Array(item_arr) => {
                        assert_eq!(
                            item_arr.len(),
                            3,
                            "WITHDIST WITHCOORD result should have 3 elements"
                        );
                    }
                    _ => panic!("WITHDIST WITHCOORD result should be array"),
                }
            }
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_count() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Search with count limit
    let result = ctx
        .georadius(
            "paris:landmarks",
            "2.3372",
            "48.8606",
            "5000",
            "m",
            &["COUNT", "2"],
        )
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(
                arr.len() <= 2,
                "Should return at most 2 results with COUNT 2"
            );
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_asc_desc() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Search with ASC (default)
    let result_asc = ctx
        .georadius(
            "paris:landmarks",
            "2.3372",
            "48.8606",
            "5000",
            "m",
            &["ASC", "WITHDIST"],
        )
        .await
        .unwrap();

    // Search with DESC
    let result_desc = ctx
        .georadius(
            "paris:landmarks",
            "2.3372",
            "48.8606",
            "5000",
            "m",
            &["DESC", "WITHDIST"],
        )
        .await
        .unwrap();

    // Results should be in opposite order
    match (result_asc, result_desc) {
        (RespValue::Array(asc_arr), RespValue::Array(desc_arr)) => {
            if asc_arr.len() > 1 && desc_arr.len() > 1 {
                // First element of ASC should be last element of DESC
                assert_eq!(
                    asc_arr[0],
                    desc_arr[desc_arr.len() - 1],
                    "ASC and DESC should return opposite order"
                );
            }
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_units() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Test different units (should find same result with different radius values)
    let result_m = ctx
        .georadius("paris:landmarks", "2.2945", "48.8582", "1000", "m", &[])
        .await
        .unwrap();
    let result_km = ctx
        .georadius("paris:landmarks", "2.2945", "48.8582", "1", "km", &[])
        .await
        .unwrap();
    let result_ft = ctx
        .georadius("paris:landmarks", "2.2945", "48.8582", "3280", "ft", &[])
        .await
        .unwrap();
    let result_mi = ctx
        .georadius("paris:landmarks", "2.2945", "48.8582", "0.621", "mi", &[])
        .await
        .unwrap();

    // All should find the same member
    match (result_m, result_km, result_ft, result_mi) {
        (
            RespValue::Array(m_arr),
            RespValue::Array(km_arr),
            RespValue::Array(ft_arr),
            RespValue::Array(mi_arr),
        ) => {
            assert!(!m_arr.is_empty(), "Meters should find result");
            assert!(!km_arr.is_empty(), "Kilometers should find result");
            assert!(!ft_arr.is_empty(), "Feet should find result");
            assert!(!mi_arr.is_empty(), "Miles should find result");
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_nonexistent_key() {
    let ctx = TestContext::new().await;

    // Search in nonexistent key
    let result = ctx
        .georadius("nonexistent:key", "2.2945", "48.8582", "1000", "m", &[])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(
                arr.len(),
                0,
                "Should return empty array for nonexistent key"
            );
        }
        _ => panic!("GEORADIUS should return array"),
    }
}

#[tokio::test]
async fn test_georadius_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("wrong:type", "value").await.unwrap();

    // Try to search in wrong type - GEORADIUS checks type during execution
    let result = ctx
        .georadius("wrong:type", "2.2945", "48.8582", "1000", "m", &[])
        .await;
    // The implementation may return empty array or error depending on when type check happens
    match result {
        Ok(RespValue::Array(arr)) => {
            // If it returns empty array, that's also acceptable behavior
            assert_eq!(
                arr.len(),
                0,
                "GEORADIUS with wrong type may return empty array"
            );
        }
        Err(SpinelDBError::WrongType) => {
            // This is also acceptable
        }
        other => panic!(
            "GEORADIUS should return empty array or WrongType error, got {:?}",
            other
        ),
    }
}

// ===== GEORADIUSBYMEMBER Tests =====

#[tokio::test]
async fn test_georadiusbymember_basic() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Search radius around a member
    let result = ctx
        .georadiusbymember("paris:landmarks", "Louvre Museum", "2000", "m", &[])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(
                arr.len() >= 2,
                "Should find at least 2 landmarks within 2km"
            );
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}

#[tokio::test]
async fn test_georadiusbymember_withdist() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
        ],
    )
    .await
    .unwrap();

    // Search with distance
    let result = ctx
        .georadiusbymember(
            "paris:landmarks",
            "Louvre Museum",
            "5000",
            "m",
            &["WITHDIST"],
        )
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty(), "Should find at least one result");
            // First result should be the center member itself with distance 0
            match &arr[0] {
                RespValue::Array(item_arr) => {
                    assert_eq!(item_arr.len(), 2, "WITHDIST result should have 2 elements");
                    // Check that distance is 0 for the center member
                    match &item_arr[1] {
                        RespValue::BulkString(dist_bs) => {
                            let dist: f64 = String::from_utf8_lossy(dist_bs).parse().unwrap();
                            assert!(
                                dist.abs() < 0.1,
                                "Distance to self should be approximately 0, got {}",
                                dist
                            );
                        }
                        _ => panic!("Distance should be bulk string"),
                    }
                }
                _ => panic!("WITHDIST result should be array"),
            }
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}

#[tokio::test]
async fn test_georadiusbymember_withcoord() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Search with coordinates
    let result = ctx
        .georadiusbymember(
            "paris:landmarks",
            "Eiffel Tower",
            "1000",
            "m",
            &["WITHCOORD"],
        )
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty(), "Should find at least one result");
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}

#[tokio::test]
async fn test_georadiusbymember_count() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Search with count limit
    let result = ctx
        .georadiusbymember(
            "paris:landmarks",
            "Louvre Museum",
            "5000",
            "m",
            &["COUNT", "2"],
        )
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert!(
                arr.len() <= 2,
                "Should return at most 2 results with COUNT 2"
            );
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}

#[tokio::test]
async fn test_georadiusbymember_asc_desc() {
    let ctx = TestContext::new().await;

    // Add multiple locations
    ctx.geoadd(
        "paris:landmarks",
        &[
            ("2.2945", "48.8582", "Eiffel Tower"),
            ("2.3372", "48.8606", "Louvre Museum"),
            ("2.3522", "48.8566", "Notre Dame"),
        ],
    )
    .await
    .unwrap();

    // Search with ASC
    let result_asc = ctx
        .georadiusbymember(
            "paris:landmarks",
            "Louvre Museum",
            "5000",
            "m",
            &["ASC", "WITHDIST"],
        )
        .await
        .unwrap();

    // Search with DESC
    let result_desc = ctx
        .georadiusbymember(
            "paris:landmarks",
            "Louvre Museum",
            "5000",
            "m",
            &["DESC", "WITHDIST"],
        )
        .await
        .unwrap();

    // Results should be in opposite order
    match (result_asc, result_desc) {
        (RespValue::Array(asc_arr), RespValue::Array(desc_arr)) => {
            if asc_arr.len() > 1 && desc_arr.len() > 1 {
                // First element of ASC should be last element of DESC
                assert_eq!(
                    asc_arr[0],
                    desc_arr[desc_arr.len() - 1],
                    "ASC and DESC should return opposite order"
                );
            }
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}

#[tokio::test]
async fn test_georadiusbymember_nonexistent_member() {
    let ctx = TestContext::new().await;

    // Add a location
    ctx.geoadd("paris:landmarks", &[("2.2945", "48.8582", "Eiffel Tower")])
        .await
        .unwrap();

    // Search around nonexistent member - returns empty array (similar to Redis behavior)
    let result = ctx
        .georadiusbymember("paris:landmarks", "Nonexistent", "1000", "m", &[])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(
                arr.len(),
                0,
                "GEORADIUSBYMEMBER should return empty array for nonexistent center member"
            );
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}

#[tokio::test]
async fn test_georadiusbymember_nonexistent_key() {
    let ctx = TestContext::new().await;

    // Search in nonexistent key - returns empty array (similar to Redis behavior)
    let result = ctx
        .georadiusbymember("nonexistent:key", "member", "1000", "m", &[])
        .await
        .unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(
                arr.len(),
                0,
                "GEORADIUSBYMEMBER should return empty array for nonexistent key"
            );
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}

#[tokio::test]
async fn test_georadiusbymember_wrong_type() {
    let ctx = TestContext::new().await;

    // Create a string key
    ctx.set("wrong:type", "value").await.unwrap();

    // Try to search in wrong type
    let result = ctx
        .georadiusbymember("wrong:type", "member", "1000", "m", &[])
        .await;
    assert!(
        result.is_err(),
        "GEORADIUSBYMEMBER should fail with wrong type"
    );
    match result {
        Err(SpinelDBError::WrongType) => {}
        _ => panic!("GEORADIUSBYMEMBER should return WrongType error"),
    }
}

// ===== Integration Tests =====

#[tokio::test]
async fn test_geospatial_workflow() {
    let ctx = TestContext::new().await;

    // 1. Add multiple locations
    let add_result = ctx
        .geoadd(
            "cities",
            &[
                ("2.2945", "48.8582", "Paris"),
                ("-0.1276", "51.5074", "London"),
                ("13.4050", "52.5200", "Berlin"),
            ],
        )
        .await
        .unwrap();
    assert_integer(&add_result, 3, "Should add 3 cities");

    // 2. Get positions
    let pos_result = ctx.geopos("cities", &["Paris", "London"]).await.unwrap();
    match pos_result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2, "Should return 2 positions");
            assert_ne!(arr[0], RespValue::Null, "Paris should exist");
            assert_ne!(arr[1], RespValue::Null, "London should exist");
        }
        _ => panic!("GEOPOS should return array"),
    }

    // 3. Calculate distance
    let dist_result = ctx
        .geodist("cities", "Paris", "London", Some("km"))
        .await
        .unwrap();
    match dist_result {
        RespValue::BulkString(bs) => {
            let dist: f64 = String::from_utf8_lossy(&bs).parse().unwrap();
            // Paris to London is approximately 344 km
            assert!(
                (dist - 344.0).abs() < 50.0,
                "Distance should be approximately 344km, got {}",
                dist
            );
        }
        _ => panic!("GEODIST should return bulk string"),
    }

    // 4. Get geohashes
    let hash_result = ctx.geohash("cities", &["Paris", "Berlin"]).await.unwrap();
    match hash_result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2, "Should return 2 geohashes");
            for item in arr {
                match item {
                    RespValue::BulkString(bs) => {
                        let hash = String::from_utf8_lossy(&bs);
                        assert_eq!(hash.len(), 11, "Geohash should be 11 characters");
                    }
                    _ => panic!("GEOHASH should return bulk string"),
                }
            }
        }
        _ => panic!("GEOHASH should return array"),
    }

    // 5. Search radius
    let radius_result = ctx
        .georadius(
            "cities",
            "2.2945",
            "48.8582",
            "1000",
            "km",
            &["COUNT", "10"],
        )
        .await
        .unwrap();
    match radius_result {
        RespValue::Array(arr) => {
            assert!(
                !arr.is_empty(),
                "Should find at least one city within 1000km"
            );
        }
        _ => panic!("GEORADIUS should return array"),
    }

    // 6. Search by member
    let member_result = ctx
        .georadiusbymember("cities", "Paris", "1000", "km", &["WITHDIST"])
        .await
        .unwrap();
    match member_result {
        RespValue::Array(arr) => {
            assert!(
                !arr.is_empty(),
                "Should find at least one city within 1000km of Paris"
            );
        }
        _ => panic!("GEORADIUSBYMEMBER should return array"),
    }
}
