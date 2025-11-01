use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct FtSynonymDumpCommand {
    pub index_name: String,
}

#[async_trait]
impl ExecutableCommand for FtSynonymDumpCommand {
    async fn execute<'a>(
        &self,
        _ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Placeholder for dumping synonyms
        // This would return all synonym groups in the index
        let result = vec![
            RespValue::Array(vec![]), // No synonyms for now
        ];

        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

impl FtSynonymDumpCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("FT.SYNDUMP".to_string()));
        }

        let index_name = args[0].string_from_bytes()?;

        Ok(Self { index_name })
    }
}

impl CommandSpec for FtSynonymDumpCommand {
    fn name(&self) -> &'static str {
        "ft.syndump"
    }

    fn arity(&self) -> i64 {
        2 // FT.SYNDUMP index_name
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
            Bytes::from_static(b"SYNDUMP"),
            Bytes::from(self.index_name.clone()),
        ]
    }
}
