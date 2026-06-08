// src/core/commands/geospatial/georadius.rs

use super::helpers::{self, GeoPoint, GeoUnit, haversine_distance, score_to_coordinates};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::zset::{ScoreBoundary, SortedSet};
use crate::core::database::{Db, ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use geohash;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::MutexGuard;

// --- Options Structs ---

/// Holds the optional arguments for GEORADIUS commands.
#[derive(Debug, Clone, Default)]
pub struct RadiusOptions {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    sort_asc: bool,
    store: Option<Bytes>,
    store_dist: Option<Bytes>,
}

/// Represents the center point for a radius search, either by coordinates or a member.
#[derive(Debug, Clone)]
pub enum GeoRadiusCenter {
    Coord(f64, f64),
    Member(Bytes),
}

// --- Main Command Struct ---

/// Represents the parsed GEORADIUS or GEORADIUSBYMEMBER command.
#[derive(Debug, Clone)]
pub struct GeoRadius {
    pub key: Bytes,
    pub center: GeoRadiusCenter,
    pub radius: f64,
    pub unit: GeoUnit,
    pub options: RadiusOptions,
}

impl Default for GeoRadius {
    fn default() -> Self {
        Self {
            key: Bytes::new(),
            center: GeoRadiusCenter::Coord(0.0, 0.0),
            radius: 0.0,
            unit: GeoUnit::Meters,
            options: RadiusOptions::default(),
        }
    }
}

// --- Shared Implementation ---

impl GeoRadius {
    /// Shared parsing logic for both GEORADIUS and GEORADIUSBYMEMBER.
    pub fn parse_shared(args: &[RespFrame], is_by_member: bool) -> Result<Self, SpinelDBError> {
        let cmd_name = if is_by_member {
            "GEORADIUSBYMEMBER"
        } else {
            "GEORADIUS"
        };
        let min_args = if is_by_member { 4 } else { 5 };
        if args.len() < min_args {
            return Err(SpinelDBError::WrongArgumentCount(cmd_name.to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let (center, mut i) = if is_by_member {
            (GeoRadiusCenter::Member(extract_bytes(&args[1])?), 2)
        } else {
            let lon = extract_string(&args[1])?
                .parse::<f64>()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            let lat = extract_string(&args[2])?
                .parse::<f64>()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            (GeoRadiusCenter::Coord(lon, lat), 3)
        };

        let radius = extract_string(&args[i])?
            .parse()
            .map_err(|_| SpinelDBError::NotAFloat)?;
        i += 1;
        let unit = GeoUnit::from_str(&extract_string(&args[i])?)?;
        i += 1;

        let mut options = RadiusOptions {
            sort_asc: true, // Default sort order is ASC
            ..Default::default()
        };
        while i < args.len() {
            match extract_string(&args[i])?.to_ascii_lowercase().as_str() {
                "withcoord" => options.with_coord = true,
                "withdist" => options.with_dist = true,
                "withhash" => options.with_hash = true,
                "count" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    options.count = Some(extract_string(&args[i])?.parse()?);
                }
                "asc" => options.sort_asc = true,
                "desc" => options.sort_asc = false,
                "store" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    options.store = Some(extract_bytes(&args[i])?);
                }
                "storedist" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    options.store_dist = Some(extract_bytes(&args[i])?);
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
            i += 1;
        }
        Ok(GeoRadius {
            key,
            center,
            radius,
            unit,
            options,
        })
    }

    /// Shared execution logic for both radius commands.
    pub async fn execute_shared(
        &self,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let db = ctx.db;
        let mut guards = match std::mem::replace(&mut ctx.locks, ExecutionLocks::None) {
            ExecutionLocks::Multi { guards } => guards,
            ExecutionLocks::Single { shard_index, guard } => {
                let mut map = BTreeMap::new();
                map.insert(shard_index, guard);
                map
            }
            _ => {
                return Err(SpinelDBError::Internal(
                    "GEORADIUS requires appropriate lock (Single or Multi)".into(),
                ));
            }
        };

        let results = self.get_members_in_radius(&guards, db).await?;

        if self.options.store.is_some() || self.options.store_dist.is_some() {
            self.execute_store(results, &mut guards, db).await
        } else {
            self.format_results(results)
        }
    }

    /// Handles storing the results in a destination key if STORE or STOREDIST is used.
    async fn execute_store<'a>(
        &self,
        results: Vec<GeoPoint>,
        guards: &mut BTreeMap<usize, MutexGuard<'a, crate::core::database::ShardCache>>,
        db: &Db,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut keys_modified = 0;
        let len = results.len() as i64;

        if let Some(key) = &self.options.store {
            let mut new_zset = SortedSet::new();
            for point in &results {
                new_zset.add(
                    point
                        .score
                        .expect("GeoPoint score should always be present"),
                    point.member.clone(),
                );
            }
            let new_val = StoredValue::new(DataValue::SortedSet(new_zset));
            let shard_index = db.get_shard_index(key);
            if let Some(guard) = guards.get_mut(&shard_index) {
                guard.put(key.clone(), new_val);
                keys_modified += 1;
            }
        }
        if let Some(key) = &self.options.store_dist {
            let mut new_zset = SortedSet::new();
            for point in &results {
                new_zset.add(
                    point
                        .dist
                        .expect("GeoPoint distance should always be present"),
                    point.member.clone(),
                );
            }
            let new_val = StoredValue::new(DataValue::SortedSet(new_zset));
            let shard_index = db.get_shard_index(key);
            if let Some(guard) = guards.get_mut(&shard_index) {
                guard.put(key.clone(), new_val);
                keys_modified += 1;
            }
        }
        Ok((
            RespValue::Integer(len),
            WriteOutcome::Write { keys_modified },
        ))
    }

    /// Formats the query results into the final RESP response.
    fn format_results(
        &self,
        results: Vec<GeoPoint>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut resp_array = Vec::new();
        for point in results {
            if !self.options.with_coord && !self.options.with_dist && !self.options.with_hash {
                resp_array.push(RespValue::BulkString(point.member));
            } else {
                let mut item_array = vec![RespValue::BulkString(point.member)];
                if self.options.with_dist {
                    item_array.push(RespValue::BulkString(
                        point
                            .dist
                            .expect("GeoPoint distance should always be present")
                            .to_string()
                            .into(),
                    ));
                }
                if self.options.with_hash {
                    item_array.push(RespValue::Integer(
                        point
                            .score
                            .expect("GeoPoint score should always be present")
                            as i64,
                    ));
                }
                if self.options.with_coord {
                    let (lon, lat) = point
                        .coords
                        .expect("GeoPoint coordinates should always be present");
                    item_array.push(RespValue::Array(vec![
                        RespValue::BulkString(lon.to_string().into()),
                        RespValue::BulkString(lat.to_string().into()),
                    ]));
                }
                resp_array.push(RespValue::Array(item_array));
            }
        }
        Ok((RespValue::Array(resp_array), WriteOutcome::DidNotWrite))
    }

    /// Performs the main geo query logic using geohashes.
    async fn get_members_in_radius<'a>(
        &self,
        guards: &BTreeMap<usize, MutexGuard<'a, crate::core::database::ShardCache>>,
        db: &Db,
    ) -> Result<Vec<GeoPoint>, SpinelDBError> {
        let (center_lon, center_lat) = match &self.center {
            GeoRadiusCenter::Coord(lon, lat) => (*lon, *lat),
            GeoRadiusCenter::Member(member) => {
                let shard_index = db.get_shard_index(&self.key);
                let guard = guards
                    .get(&shard_index)
                    .ok_or(SpinelDBError::Internal("Missing source lock".into()))?;

                if let Some(entry) = guard.peek(&self.key) {
                    if let DataValue::SortedSet(zset) = &entry.data {
                        if let Some(score) = zset.get_score(member) {
                            score_to_coordinates(score)?
                        } else {
                            return Ok(vec![]);
                        }
                    } else {
                        return Err(SpinelDBError::WrongType);
                    }
                } else {
                    return Ok(vec![]);
                }
            }
        };

        let radius_meters = self.radius
            * match self.unit {
                GeoUnit::Meters => 1.0,
                GeoUnit::Kilometers => 1000.0,
                GeoUnit::Feet => 0.3048,
                GeoUnit::Miles => 1609.34,
            };
        let step = helpers::radius_to_geohash_step(radius_meters);

        let center_coord = geohash::Coord {
            x: center_lon,
            y: center_lat,
        };
        let center_hash_str = geohash::encode(center_coord, step)
            .map_err(|e| SpinelDBError::Internal(e.to_string()))?;
        let neighbors = geohash::neighbors(&center_hash_str)
            .map_err(|e| SpinelDBError::Internal(e.to_string()))?;
        let areas_to_search = [
            center_hash_str,
            neighbors.n,
            neighbors.ne,
            neighbors.e,
            neighbors.se,
            neighbors.s,
            neighbors.sw,
            neighbors.w,
            neighbors.nw,
        ];

        let mut candidates = HashMap::new();
        let source_shard_index = db.get_shard_index(&self.key);
        if let Some(guard) = guards.get(&source_shard_index) {
            if let Some(entry) = guard.peek(&self.key)
                && !entry.is_expired()
                && let DataValue::SortedSet(zset) = &entry.data
            {
                for area_hash in &areas_to_search {
                    let (min_score, max_score) = helpers::geohash_to_score_range(area_hash)?;
                    let range_results = zset.get_range_by_score(
                        ScoreBoundary::Inclusive(min_score),
                        ScoreBoundary::Inclusive(max_score),
                    );
                    for item in range_results {
                        candidates.insert(item.member.clone(), item);
                    }
                }
            }
        } else {
            return Ok(vec![]);
        }

        let mut final_results = Vec::new();
        for item in candidates.values() {
            let (item_lon, item_lat) = helpers::score_to_coordinates(item.score)?;
            let dist_meters =
                haversine_distance(center_lon, center_lat, item_lon, item_lat, GeoUnit::Meters);

            if dist_meters <= radius_meters {
                let dist_in_unit =
                    haversine_distance(center_lon, center_lat, item_lon, item_lat, self.unit);
                final_results.push(GeoPoint {
                    member: item.member.clone(),
                    dist: Some(dist_in_unit),
                    score: Some(item.score),
                    coords: Some((item_lon, item_lat)),
                });
            }
        }

        final_results.sort_by(|a, b| {
            a.dist
                .expect("GeoPoint distance should always be present")
                .partial_cmp(&b.dist.expect("GeoPoint distance should always be present"))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if !self.options.sort_asc {
            final_results.reverse();
        }
        if let Some(count) = self.options.count {
            final_results.truncate(count);
        }

        Ok(final_results)
    }

    /// Determines command flags based on whether a STORE option is used.
    fn options_to_flags(&self) -> CommandFlags {
        if self.options.store.is_some() || self.options.store_dist.is_some() {
            CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
        } else {
            CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
        }
    }

    /// Extracts all keys involved in the command, including destination keys.
    fn options_to_keys(&self) -> Vec<Bytes> {
        let mut keys = vec![self.key.clone()];
        if let Some(k) = &self.options.store {
            keys.push(k.clone());
        }
        if let Some(k) = &self.options.store_dist {
            keys.push(k.clone());
        }
        keys
    }
}

// --- Wrapper Structs for Dispatch ---

/// Wrapper for the GEORADIUS command.
#[derive(Debug, Clone, Default)]
pub struct GeoRadiusCmd(pub GeoRadius);
impl ParseCommand for GeoRadiusCmd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        Ok(Self(GeoRadius::parse_shared(args, false)?))
    }
}
#[async_trait]
impl ExecutableCommand for GeoRadiusCmd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        self.0.execute_shared(ctx).await
    }
}
impl CommandSpec for GeoRadiusCmd {
    fn name(&self) -> &'static str {
        "georadius"
    }
    fn arity(&self) -> i64 {
        -6
    }
    fn flags(&self) -> CommandFlags {
        self.0.options_to_flags()
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.0.options_to_keys()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.0.key.clone()]
    }
}

/// Wrapper for the GEORADIUSBYMEMBER command.
#[derive(Debug, Clone, Default)]
pub struct GeoRadiusByMemberCmd(pub GeoRadius);
impl ParseCommand for GeoRadiusByMemberCmd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        Ok(Self(GeoRadius::parse_shared(args, true)?))
    }
}
#[async_trait]
impl ExecutableCommand for GeoRadiusByMemberCmd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        self.0.execute_shared(ctx).await
    }
}
impl CommandSpec for GeoRadiusByMemberCmd {
    fn name(&self) -> &'static str {
        "georadiusbymember"
    }
    fn arity(&self) -> i64 {
        -5
    }
    fn flags(&self) -> CommandFlags {
        self.0.options_to_flags()
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.0.options_to_keys()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.0.key.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_georadius_parses_basic() {
        let c =
            GeoRadiusCmd::parse(&[bs("k"), bs("10.0"), bs("20.0"), bs("100"), bs("km")]).unwrap();
        assert_eq!(c.0.key, Bytes::from_static(b"k"));
        assert!(
            matches!(c.0.center, GeoRadiusCenter::Coord(lon, lat) if (lon - 10.0).abs() < f64::EPSILON && (lat - 20.0).abs() < f64::EPSILON)
        );
        assert!((c.0.radius - 100.0).abs() < f64::EPSILON);
        assert!(matches!(c.0.unit, GeoUnit::Kilometers));
    }

    #[test]
    fn test_georadius_with_withcoord() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("WITHCOORD"),
        ])
        .unwrap();
        assert!(c.0.options.with_coord);
    }

    #[test]
    fn test_georadius_with_withdist() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("WITHDIST"),
        ])
        .unwrap();
        assert!(c.0.options.with_dist);
    }

    #[test]
    fn test_georadius_with_withhash() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("WITHHASH"),
        ])
        .unwrap();
        assert!(c.0.options.with_hash);
    }

    #[test]
    fn test_georadius_with_count() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("COUNT"),
            bs("5"),
        ])
        .unwrap();
        assert_eq!(c.0.options.count, Some(5));
    }

    #[test]
    fn test_georadius_with_asc() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("ASC"),
        ])
        .unwrap();
        assert!(c.0.options.sort_asc);
    }

    #[test]
    fn test_georadius_with_desc() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("DESC"),
        ])
        .unwrap();
        assert!(!c.0.options.sort_asc);
    }

    #[test]
    fn test_georadius_with_store() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("STORE"),
            bs("dst"),
        ])
        .unwrap();
        assert_eq!(c.0.options.store, Some(Bytes::from_static(b"dst")));
    }

    #[test]
    fn test_georadius_with_storedist() {
        let c = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("STOREDIST"),
            bs("dst"),
        ])
        .unwrap();
        assert_eq!(c.0.options.store_dist, Some(Bytes::from_static(b"dst")));
    }

    #[test]
    fn test_georadius_with_too_few_args_is_error() {
        let r = GeoRadiusCmd::parse(&[bs("k"), bs("10.0"), bs("20.0"), bs("100")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_georadius_unknown_option_is_error() {
        let r = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("FOO"),
        ]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_georadius_count_without_value_is_syntax_error() {
        let r = GeoRadiusCmd::parse(&[
            bs("k"),
            bs("10.0"),
            bs("20.0"),
            bs("100"),
            bs("km"),
            bs("COUNT"),
        ]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_georadius_invalid_lon_is_error() {
        let r = GeoRadiusCmd::parse(&[bs("k"), bs("not_a_float"), bs("20.0"), bs("100"), bs("km")]);
        assert!(matches!(r, Err(SpinelDBError::NotAFloat)));
    }

    #[test]
    fn test_georadius_radiusbymember_parses() {
        let c = GeoRadiusByMemberCmd::parse(&[bs("k"), bs("m1"), bs("100"), bs("km")]).unwrap();
        assert!(matches!(c.0.center, GeoRadiusCenter::Member(m) if m == Bytes::from_static(b"m1")));
    }

    #[test]
    fn test_georadius_radiusbymember_too_few_args_is_error() {
        let r = GeoRadiusByMemberCmd::parse(&[bs("k"), bs("m1"), bs("100")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_georadius_to_resp_args_includes_source_key() {
        let c =
            GeoRadiusCmd::parse(&[bs("k"), bs("10.0"), bs("20.0"), bs("100"), bs("km")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args, vec![Bytes::from_static(b"k")]);
    }
}
