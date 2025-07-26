// src/core/commands/cache/cache_info.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{CacheBody, DataValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub struct CacheInfo {
    pub key: Bytes,
}

impl ParseCommand for CacheInfo {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "CACHE.INFO")?;
        Ok(CacheInfo {
            key: extract_bytes(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for CacheInfo {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;
        let Some(entry) = guard.peek(&self.key) else {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };
        if entry.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        if let DataValue::HttpCache {
            variants, vary_on, ..
        } = &entry.data
        {
            let mut info = Vec::new();
            let now = Instant::now();

            if let Some(exp) = entry.expiry {
                info.push(RespValue::BulkString("ttl".into()));
                info.push(RespValue::Integer(
                    exp.saturating_duration_since(now).as_secs() as i64,
                ));
            }
            if let Some(exp) = entry.stale_revalidate_expiry {
                info.push(RespValue::BulkString("swr_ttl".into()));
                info.push(RespValue::Integer(
                    exp.saturating_duration_since(now).as_secs() as i64,
                ));
            }
            if let Some(exp) = entry.grace_expiry {
                info.push(RespValue::BulkString("grace_ttl".into()));
                info.push(RespValue::Integer(
                    exp.saturating_duration_since(now).as_secs() as i64,
                ));
            }
            info.push(RespValue::BulkString("variants_count".into()));
            info.push(RespValue::Integer(variants.len() as i64));

            let vary_on_str = vary_on
                .iter()
                .map(|b| String::from_utf8_lossy(b))
                .collect::<Vec<_>>()
                .join(", ");
            info.push(RespValue::BulkString("vary_on".into()));
            info.push(RespValue::BulkString(vary_on_str.into()));

            let variants_info: Vec<RespValue> = variants
                .iter()
                .map(|(hash, variant)| {
                    let mut variant_details = vec![
                        RespValue::BulkString("hash".into()),
                        RespValue::BulkString(hash.to_string().into()),
                        RespValue::BulkString("size".into()),
                        RespValue::Integer(variant.body.len() as i64),
                        RespValue::BulkString("storage".into()),
                        RespValue::BulkString(
                            (if matches!(variant.body, CacheBody::InMemory(_)) {
                                "memory"
                            } else {
                                "disk"
                            })
                            .into(),
                        ),
                    ];
                    if let Some(etag) = &variant.metadata.etag {
                        variant_details.push(RespValue::BulkString("etag".into()));
                        variant_details.push(RespValue::BulkString(etag.clone()));
                    }
                    variant_details
                })
                .map(RespValue::Array)
                .collect();
            info.push(RespValue::BulkString("variants".into()));
            info.push(RespValue::Array(variants_info));

            return Ok((RespValue::Array(info), WriteOutcome::DidNotWrite));
        }

        Err(SpinelDBError::WrongType)
    }
}

impl CommandSpec for CacheInfo {
    fn name(&self) -> &'static str {
        "cache.info"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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
        vec![self.key.clone()]
    }
}
