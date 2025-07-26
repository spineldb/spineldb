use super::helpers::score_to_coordinates;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct GeoPos {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ParseCommand for GeoPos {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("GEOPOS".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let members = args[1..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;
        Ok(GeoPos { key, members })
    }
}

#[async_trait]
impl ExecutableCommand for GeoPos {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        let mut results = Vec::with_capacity(self.members.len());

        if let Some(entry) = shard_cache_guard.get(&self.key) {
            if !entry.is_expired() {
                if let DataValue::SortedSet(zset) = &entry.data {
                    for member in &self.members {
                        if let Some(score) = zset.get_score(member) {
                            match score_to_coordinates(score) {
                                Ok((lon, lat)) => results.push(RespValue::Array(vec![
                                    RespValue::BulkString(lon.to_string().into()),
                                    RespValue::BulkString(lat.to_string().into()),
                                ])),
                                Err(_) => results.push(RespValue::Null),
                            }
                        } else {
                            results.push(RespValue::Null);
                        }
                    }
                } else {
                    return Err(SpinelDBError::WrongType);
                }
            } else {
                for _ in &self.members {
                    results.push(RespValue::Null);
                }
            }
        } else {
            for _ in &self.members {
                results.push(RespValue::Null);
            }
        }

        Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for GeoPos {
    fn name(&self) -> &'static str {
        "geopos"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone()];
        args.extend(self.members.clone());
        args
    }
}
