// src/core/commands/search/exec.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::search::query::QueryParser;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::core::NUM_SHARDS;
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct FtSearchCommand {
    pub index_name: String,
    pub query: String,
}

#[async_trait]
impl ExecutableCommand for FtSearchCommand {
    async fn execute<'a>(
        &self,
        ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let index = ctx
            .state
            .search_indexes
            .get(&self.index_name)
            .ok_or_else(|| SpinelDBError::Internal("Index does not exist".to_string()))?;

        let query = QueryParser::parse(&self.query)?;

        let mut matching_docs = Vec::new();
        let prefix = index.prefix.clone();

        for shard_idx in 0..NUM_SHARDS {
            let shard = ctx.db.get_shard(shard_idx);
            let guard = shard.entries.lock().await;

            for (key, stored_value) in guard.iter() {
                if key.starts_with(prefix.as_bytes())
                    && let DataValue::Hash(hash_map) = &stored_value.data
                    && !stored_value.is_expired()
                {
                    let mut fields = HashMap::new();
                    for (field_bytes, value_bytes) in hash_map.iter() {
                        fields.insert(
                            field_bytes.string_from_bytes().unwrap_or_default(),
                            value_bytes
                                .string_from_bytes()
                                .unwrap_or_default()
                                .to_lowercase(),
                        );
                    }

                    if query.matches(&fields, &index.schema) {
                        matching_docs.push(RespValue::BulkString(SpinelString::from(key.clone())));
                        let mut hash_resp_array = Vec::new();
                        for (field_bytes, value_bytes) in hash_map.iter() {
                            hash_resp_array.push(RespValue::BulkString(field_bytes.clone()));
                            hash_resp_array.push(RespValue::BulkString(value_bytes.clone()));
                        }
                        matching_docs.push(RespValue::Array(hash_resp_array));
                    }
                }
            }
        }

        let mut result = vec![RespValue::Integer(matching_docs.len() as i64 / 2)];
        result.extend(matching_docs);

        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

impl FtSearchCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("FT.SEARCH".to_string()));
        }
        let index_name = args[0].string_from_bytes()?;
        let query = args[1].string_from_bytes()?;
        // In a real implementation, you'd parse the rest of the args (e.g., LIMIT, SORTBY)
        Ok(Self { index_name, query })
    }
}

impl CommandSpec for FtSearchCommand {
    fn name(&self) -> &'static str {
        "ft.search"
    }

    fn arity(&self) -> i64 {
        -3 // FT.SEARCH index_name query ...
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
        0
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![] // This command doesn't have keys in the traditional sense
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            Bytes::from_static(b"SEARCH"),
            Bytes::from(self.index_name.clone()),
            Bytes::from(self.query.clone()),
        ]
    }
}
