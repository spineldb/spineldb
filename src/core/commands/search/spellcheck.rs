use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct FtSpellCheckCommand {
    pub index_name: String,
    pub query: String,
}

#[async_trait]
impl ExecutableCommand for FtSpellCheckCommand {
    async fn execute<'a>(
        &self,
        _ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // For now, return an empty spellcheck result
        // In a real implementation, this would check for typos and suggest corrections
        let result = vec![RespValue::Array(vec![
            RespValue::BulkString(self.query.clone().into()),
            RespValue::Array(vec![]), // No suggestions for now
        ])];

        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

impl FtSpellCheckCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount(
                "FT.SPELLCHECK".to_string(),
            ));
        }

        let index_name = args[0].string_from_bytes()?;
        let query = args[1].string_from_bytes()?;

        Ok(Self { index_name, query })
    }
}

impl CommandSpec for FtSpellCheckCommand {
    fn name(&self) -> &'static str {
        "ft.spellcheck"
    }

    fn arity(&self) -> i64 {
        -3 // FT.SPELLCHECK index_name query ...
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
            Bytes::from_static(b"SPELLCHECK"),
            Bytes::from(self.index_name.clone()),
            Bytes::from(self.query.clone()),
        ]
    }
}
