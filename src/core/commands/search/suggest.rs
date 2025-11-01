use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct FtSuggestCommand {
    pub index_name: String,
    pub prefix: String,
    pub fuzzy: bool,
    pub num: usize,
}

impl Default for FtSuggestCommand {
    fn default() -> Self {
        Self {
            index_name: String::new(),
            prefix: String::new(),
            fuzzy: false,
            num: 5,
        }
    }
}

#[async_trait]
impl ExecutableCommand for FtSuggestCommand {
    async fn execute<'a>(
        &self,
        _ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Placeholder for suggestion functionality
        // This would look up terms that start with the given prefix
        let result = vec![
            RespValue::Array(vec![]), // No suggestions for now
        ];

        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

impl FtSuggestCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("FT.SUGGET".to_string()));
        }

        let index_name = args[0].string_from_bytes()?;
        let prefix = args[1].string_from_bytes()?;

        let mut fuzzy = false;
        let mut num = 5;

        let mut i = 2;
        while i < args.len() {
            let arg = args[i].string_from_bytes()?.to_ascii_lowercase();
            match arg.as_str() {
                "fuzzy" => {
                    fuzzy = true;
                    i += 1;
                }
                "num" => {
                    if i + 1 >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("FT.SUGGET".to_string()));
                    }
                    num = args[i + 1]
                        .string_from_bytes()?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::SyntaxError)?;
                    i += 2;
                }
                _ => {
                    return Err(SpinelDBError::SyntaxError);
                }
            }
        }

        Ok(Self {
            index_name,
            prefix,
            fuzzy,
            num,
        })
    }
}

impl CommandSpec for FtSuggestCommand {
    fn name(&self) -> &'static str {
        "ft.suggest"
    }

    fn arity(&self) -> i64 {
        -3 // FT.SUGGEST index_name prefix ...
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
        let mut args = vec![
            Bytes::from_static(b"SUGGEST"),
            Bytes::from(self.index_name.clone()),
            Bytes::from(self.prefix.clone()),
        ];

        if self.fuzzy {
            args.push(Bytes::from_static(b"FUZZY"));
        }
        args.push(Bytes::from_static(b"NUM"));
        args.push(Bytes::from(self.num.to_string()));

        args
    }
}
