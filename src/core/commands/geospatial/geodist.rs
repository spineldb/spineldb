use super::helpers::{GeoUnit, haversine_distance, score_to_coordinates};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct GeoDist {
    pub key: Bytes,
    pub member1: Bytes,
    pub member2: Bytes,
    pub unit: GeoUnit,
}

impl Default for GeoDist {
    fn default() -> Self {
        Self {
            key: Bytes::new(),
            member1: Bytes::new(),
            member2: Bytes::new(),
            unit: GeoUnit::Meters,
        }
    }
}

impl ParseCommand for GeoDist {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 || args.len() > 4 {
            return Err(SpinelDBError::WrongArgumentCount("GEODIST".to_string()));
        }
        let unit = if args.len() == 4 {
            GeoUnit::from_str(&extract_string(&args[3])?)?
        } else {
            GeoUnit::Meters
        };
        Ok(GeoDist {
            key: extract_bytes(&args[0])?,
            member1: extract_bytes(&args[1])?,
            member2: extract_bytes(&args[2])?,
            unit,
        })
    }
}

#[async_trait]
impl ExecutableCommand for GeoDist {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        if let Some(entry) = shard_cache_guard.get(&self.key)
            && !entry.is_expired()
        {
            if let DataValue::SortedSet(zset) = &entry.data {
                if let (Some(score1), Some(score2)) =
                    (zset.get_score(&self.member1), zset.get_score(&self.member2))
                {
                    let (lon1, lat1) = score_to_coordinates(score1)?;
                    let (lon2, lat2) = score_to_coordinates(score2)?;
                    let dist = haversine_distance(lon1, lat1, lon2, lat2, self.unit);
                    return Ok((
                        RespValue::BulkString(dist.to_string().into()),
                        WriteOutcome::DidNotWrite,
                    ));
                }
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }
        Ok((RespValue::Null, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for GeoDist {
    fn name(&self) -> &'static str {
        "geodist"
    }
    fn arity(&self) -> i64 {
        -4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.key.clone(), self.member1.clone(), self.member2.clone()];
        if !matches!(self.unit, GeoUnit::Meters) {
            args.push(format!("{:?}", self.unit).to_lowercase().into());
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_geodist_parses_key_and_members() {
        let c = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.member1, Bytes::from_static(b"m1"));
        assert_eq!(c.member2, Bytes::from_static(b"m2"));
        assert!(matches!(c.unit, GeoUnit::Meters));
    }

    #[test]
    fn test_geodist_with_unit_km() {
        let c = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2"), bs("km")]).unwrap();
        assert!(matches!(c.unit, GeoUnit::Kilometers));
    }

    #[test]
    fn test_geodist_with_unit_mi() {
        let c = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2"), bs("mi")]).unwrap();
        assert!(matches!(c.unit, GeoUnit::Miles));
    }

    #[test]
    fn test_geodist_with_unit_ft() {
        let c = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2"), bs("ft")]).unwrap();
        assert!(matches!(c.unit, GeoUnit::Feet));
    }

    #[test]
    fn test_geodist_invalid_unit_is_error() {
        let r = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2"), bs("invalid")]);
        assert!(r.is_err());
    }

    #[test]
    fn test_geodist_with_too_few_args_is_error() {
        let r = GeoDist::parse(&[bs("k"), bs("m1")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_geodist_with_too_many_args_is_error() {
        let r = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2"), bs("km"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_geodist_no_args_is_error() {
        let r = GeoDist::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_geodist_with_non_bulk_key_is_wrong_type() {
        let r = GeoDist::parse(&[RespFrame::Integer(1), bs("m1"), bs("m2")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_geodist_to_resp_args_round_trips_default_unit() {
        let c = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
    }

    #[test]
    fn test_geodist_to_resp_args_includes_unit_when_non_default() {
        let c = GeoDist::parse(&[bs("k"), bs("m1"), bs("m2"), bs("km")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 4);
    }
}
