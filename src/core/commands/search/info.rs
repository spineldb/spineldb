// src/core/commands/search/info.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct FtInfoCommand {
    pub index_name: String,
}

#[async_trait]
impl ExecutableCommand for FtInfoCommand {
    async fn execute<'a>(
        &self,
        ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let search_index_arc = ctx
            .state
            .search_indexes
            .get(&self.index_name)
            .ok_or_else(|| SpinelDBError::Internal("Index does not exist".to_string()))?;
        let index = search_index_arc.lock().await;

        let mut info = Vec::new();
        info.push(RespValue::SimpleString("index_name".to_string()));
        info.push(RespValue::SimpleString(index.name.clone()));

        info.push(RespValue::SimpleString("schema".to_string()));
        let mut schema_info = Vec::new();
        for (field_name, field) in &index.schema.fields {
            schema_info.push(RespValue::SimpleString(field_name.clone()));
            schema_info.push(RespValue::SimpleString(field.field_type.to_string()));
        }
        info.push(RespValue::Array(schema_info));

        // In a real implementation, you would add more info like num_docs, memory_usage, etc.

        Ok((RespValue::Array(info), WriteOutcome::DidNotWrite))
    }
}

impl FtInfoCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("FT.INFO".to_string()));
        }
        let index_name = args[0].string_from_bytes()?;
        Ok(Self { index_name })
    }
}

impl CommandSpec for FtInfoCommand {
    fn name(&self) -> &'static str {
        "ft.info"
    }

    fn arity(&self) -> i64 {
        2 // FT.INFO index_name
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
        vec![Bytes::from(self.index_name.clone())]
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            Bytes::from_static(b"INFO"),
            Bytes::from(self.index_name.clone()),
        ]
    }
}
