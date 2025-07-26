use super::helpers::{GeoUnit, haversine_distance, score_to_coordinates};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
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
        if let Some(entry) = shard_cache_guard.get(&self.key) {
            if !entry.is_expired() {
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
