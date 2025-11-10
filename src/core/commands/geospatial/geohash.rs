// src/core/commands/geospatial/geohash.rs

use super::helpers;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct GeoHash {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ParseCommand for GeoHash {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("GEOHASH".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let members = args[1..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;
        Ok(GeoHash { key, members })
    }
}

#[async_trait]
impl ExecutableCommand for GeoHash {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        let Some(entry) = guard.peek(&self.key) else {
            // If the key doesn't exist, return an array of nils
            return Ok((
                RespValue::Array(vec![RespValue::Null; self.members.len()]),
                WriteOutcome::DidNotWrite,
            ));
        };

        if entry.is_expired() {
            return Ok((
                RespValue::Array(vec![RespValue::Null; self.members.len()]),
                WriteOutcome::DidNotWrite,
            ));
        }

        if let DataValue::SortedSet(zset) = &entry.data {
            let mut results = Vec::with_capacity(self.members.len());
            for member in &self.members {
                if let Some(score) = zset.get_score(member) {
                    let geohash_str = helpers::score_to_geohash(score)?;
                    results.push(RespValue::BulkString(geohash_str.into()));
                } else {
                    results.push(RespValue::Null);
                }
            }
            Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for GeoHash {
    fn name(&self) -> &'static str {
        "geohash"
    }
    fn arity(&self) -> i64 {
        -2
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
