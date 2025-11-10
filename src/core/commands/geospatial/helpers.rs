use crate::core::SpinelDBError;
use bytes::Bytes;
use geohash;

// Earth constants and Geohash characters
const EARTH_RADIUS_METERS: f64 = 6372797.560856;
const BASE32_CHARS: [char; 32] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k',
    'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Units for distance
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoUnit {
    Meters,
    Kilometers,
    Feet,
    Miles,
}

impl GeoUnit {
    pub fn from_str(s: &str) -> Result<Self, SpinelDBError> {
        match s.to_lowercase().as_str() {
            "m" => Ok(GeoUnit::Meters),
            "km" => Ok(GeoUnit::Kilometers),
            "ft" => Ok(GeoUnit::Feet),
            "mi" => Ok(GeoUnit::Miles),
            _ => Err(SpinelDBError::InvalidState(
                "unsupported unit provided. please use m, km, ft, mi".into(),
            )),
        }
    }
}

fn decode_hash_to_int(hash: &str) -> Result<u64, SpinelDBError> {
    let mut bits = 0u64;
    for c in hash.chars() {
        let Some(idx) = BASE32_CHARS.iter().position(|&x| x == c) else {
            return Err(SpinelDBError::Internal(format!(
                "Invalid geohash character: {c}"
            )));
        };
        bits = (bits << 5) | (idx as u64);
    }
    Ok(bits)
}

fn encode_int_to_hash(mut bits: u64, len: usize) -> Result<String, SpinelDBError> {
    let mut hash = vec![' '; len];
    for i in (0..len).rev() {
        let idx = (bits & 0x1f) as usize; // Take the lowest 5 bits
        hash[i] = BASE32_CHARS[idx];
        bits >>= 5;
    }
    Ok(hash.into_iter().collect())
}

/// Converts coordinates (longitude, latitude) to a 52-bit score for a ZSET.
pub fn coordinates_to_score(longitude: f64, latitude: f64) -> Result<f64, SpinelDBError> {
    if !(-180.0..=180.0).contains(&longitude) || !(-85.05112878..=85.05112878).contains(&latitude) {
        return Err(SpinelDBError::InvalidState(
            "invalid longitude or latitude".to_string(),
        ));
    }
    let pos = geohash::Coord {
        x: longitude,
        y: latitude,
    };
    // Precision 11 results in 55 bits, we will use the top 52 bits
    let hash_str = geohash::encode(pos, 11).map_err(|e| SpinelDBError::Internal(e.to_string()))?;
    let bits = decode_hash_to_int(&hash_str)?;
    let score_bits = bits >> 3;
    Ok(score_bits as f64)
}

/// Converts a 52-bit score from a ZSET back into (longitude, latitude).
pub fn score_to_coordinates(score: f64) -> Result<(f64, f64), SpinelDBError> {
    let bits = score as u64;
    let restored_bits = bits << 3;
    let hash = encode_int_to_hash(restored_bits, 11)?;
    let decoded = geohash::decode(&hash).map_err(|e| SpinelDBError::Internal(e.to_string()))?;
    Ok((decoded.0.x, decoded.0.y))
}

/// Converts a 52-bit score from a ZSET back into an 11-character Geohash string.
pub fn score_to_geohash(score: f64) -> Result<String, SpinelDBError> {
    let bits = score as u64;
    let restored_bits = bits << 3;
    encode_int_to_hash(restored_bits, 11)
}

/// Calculates the distance between two coordinate points using the Haversine formula.
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64, unit: GeoUnit) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    let distance_meters = EARTH_RADIUS_METERS * c;

    match unit {
        GeoUnit::Meters => distance_meters,
        GeoUnit::Kilometers => distance_meters / 1000.0,
        GeoUnit::Feet => distance_meters * 3.28084,
        GeoUnit::Miles => distance_meters * 0.000621371,
    }
}

/// Determines the geohash precision level based on the given radius.
pub fn radius_to_geohash_step(radius_meters: f64) -> usize {
    if radius_meters <= 0.075 {
        11
    } else if radius_meters <= 0.6 {
        10
    } else if radius_meters <= 2.3 {
        9
    } else if radius_meters <= 19.0 {
        8
    } else if radius_meters <= 76.0 {
        7
    } else if radius_meters <= 610.0 {
        6
    } else if radius_meters <= 2400.0 {
        5
    } else if radius_meters <= 20000.0 {
        4
    } else if radius_meters <= 78000.0 {
        3
    } else if radius_meters <= 630000.0 {
        2
    } else {
        1
    }
}

/// Converts a geohash prefix into a 52-bit score range.
pub fn geohash_to_score_range(hash_prefix: &str) -> Result<(f64, f64), SpinelDBError> {
    let precision = hash_prefix.len() * 5;
    if precision > 55 {
        return Err(SpinelDBError::Internal("Geohash prefix too long".into()));
    }

    let base_bits = decode_hash_to_int(hash_prefix)?;

    // Calculate range in 55-bit space
    let min_bits_55 = base_bits << (55 - precision);
    let max_bits_55 = min_bits_55 | ((1u64 << (55 - precision)) - 1);

    // Convert to 52-bit score space
    let min_score = (min_bits_55 >> 3) as f64;
    let max_score = (max_bits_55 >> 3) as f64;

    Ok((min_score, max_score))
}

#[derive(Debug, Clone)]
pub struct GeoPoint {
    pub member: Bytes,
    pub dist: Option<f64>,
    pub score: Option<f64>,
    pub coords: Option<(f64, f64)>,
}
