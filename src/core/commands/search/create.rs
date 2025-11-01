use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::protocol::RespFrame;
use crate::core::search::index::SearchIndex;
use crate::core::search::schema::Schema;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents the `FT.CREATE` command.
#[derive(Debug, Clone, Default)]
pub struct FtCreateCommand {
    pub index_name: String,
    pub on_type: String,
    pub prefix: String,
    pub schema: Schema,
}

impl ParseCommand for FtCreateCommand {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 7 {
            return Err(SpinelDBError::WrongArgumentCount("FT.CREATE".to_string()));
        }

        let index_name = extract_string(&args[0])?;
        let on_type = extract_string(&args[2])?;
        let prefix = extract_string(&args[5])?;

        let mut schema_args_start = 0;
        for (i, arg) in args.iter().enumerate() {
            if let RespFrame::BulkString(b) = arg
                && b.eq_ignore_ascii_case(b"SCHEMA")
            {
                schema_args_start = i + 1;
                break;
            }
        }

        if schema_args_start == 0 || schema_args_start >= args.len() {
            return Err(SpinelDBError::SyntaxError);
        }

        let schema_args_resp = &args[schema_args_start..];
        let schema_args_strings: Vec<String> = schema_args_resp
            .iter()
            .map(extract_string)
            .collect::<Result<Vec<String>, SpinelDBError>>()?;
        let schema = Schema::from_args(&schema_args_strings)?;

        Ok(Self {
            index_name,
            on_type,
            prefix,
            schema,
        })
    }
}

#[async_trait]
impl ExecutableCommand for FtCreateCommand {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if ctx.state.search_indexes.contains_key(&self.index_name) {
            return Err(SpinelDBError::Internal("Index already exists".to_string()));
        }

        let index = SearchIndex::new(
            self.index_name.clone(),
            self.prefix.clone(),
            self.schema.clone(),
        );

        ctx.state
            .search_indexes
            .insert(self.index_name.clone(), Arc::new(Mutex::new(index)));

        Ok((
            RespValue::SimpleString("OK".to_string()),
            WriteOutcome::Write { keys_modified: 0 },
        ))
    }
}

impl CommandSpec for FtCreateCommand {
    fn name(&self) -> &'static str {
        "ft.create"
    }

    fn arity(&self) -> i64 {
        -7 // FT.CREATE index_name ON HASH PREFIX 1 prefix SCHEMA field_name field_type ...
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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
        let mut args = vec![Bytes::from_static(b"CREATE")];
        args.push(Bytes::from(self.index_name.clone()));
        args.push(Bytes::from_static(b"ON"));
        args.push(Bytes::from(self.on_type.clone()));
        args.push(Bytes::from_static(b"PREFIX"));
        args.push(Bytes::from_static(b"1"));
        args.push(Bytes::from(self.prefix.clone()));
        args.push(Bytes::from_static(b"SCHEMA"));
        for (field_name, field) in &self.schema.fields {
            args.push(Bytes::from(field_name.clone()));
            args.push(Bytes::from(field.field_type.to_string()));
            for option in &field.options {
                args.push(Bytes::from(option.to_string()));
            }
        }
        args
    }
}
