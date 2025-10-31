use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::search::index::SearchIndex;
use crate::core::search::schema::Schema;
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
#[derive(Debug, Clone, Default)]
pub struct FtCreateCommand {
    pub index_name: String,
    pub prefix: String,
    pub schema: Schema,
}

#[async_trait]
impl ExecutableCommand for FtCreateCommand {
    async fn execute<'a>(
        &self,
        ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
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
            .insert(self.index_name.clone(), Arc::new(index));

        Ok((
            RespValue::SimpleString("OK".to_string()),
            WriteOutcome::DidNotWrite,
        ))
    }
}

impl FtCreateCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("FT.CREATE".to_string()));
        }

        let index_name = args[0].string_from_bytes()?;

        let mut prefix_str = String::new();
        let mut schema_args_pos = 0;
        let mut i = 1; // Start after index_name

        while i < args.len() {
            let arg_upper = args[i].to_uppercase_string();
            match arg_upper.as_str() {
                "ON" => i += 2, // Skip ON and HASH
                "PREFIX" => {
                    if i + 2 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    // Skip the count (1)
                    prefix_str = args[i + 2].string_from_bytes()?;
                    i += 3;
                }
                "SCHEMA" => {
                    schema_args_pos = i + 1;
                    break;
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
        }

        if schema_args_pos == 0 || schema_args_pos >= args.len() {
            return Err(SpinelDBError::SyntaxError);
        }

        let schema_args: Vec<String> = args[schema_args_pos..]
            .iter()
            .map(|s| {
                s.string_from_bytes()
                    .map_err(|_| SpinelDBError::SyntaxError)
            }) // Convert SpinelString to String
            .collect::<Result<Vec<String>, SpinelDBError>>()?;

        let schema = Schema::from_args(&schema_args)?;

        Ok(Self {
            index_name,
            prefix: prefix_str,
            schema,
        })
    }
}

impl CommandSpec for FtCreateCommand {
    fn name(&self) -> &'static str {
        "ft.create"
    }

    fn arity(&self) -> i64 {
        -3 // FT.CREATE index_name SCHEMA field type ...
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM
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
        let mut args = vec![Bytes::from_static(b"FT.CREATE")];
        args.push(Bytes::from(self.index_name.clone()));
        args.push(Bytes::from_static(b"SCHEMA"));
        for (field_name, field) in &self.schema.fields {
            args.push(Bytes::from(field_name.clone()));
            args.push(Bytes::from(field.field_type.to_string().to_uppercase()));
            for option in &field.options {
                args.push(Bytes::from(option.to_string().to_uppercase()));
            }
        }
        args
    }
}
