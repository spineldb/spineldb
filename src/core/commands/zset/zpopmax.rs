// src/core/commands/zset/zpopmax.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::commands::zset::zpop_logic::{PopSide, ZPop};
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZPopMax {
    pub pop_cmd: ZPop,
}

impl ParseCommand for ZPopMax {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() || args.len() > 2 {
            return Err(SpinelDBError::WrongArgumentCount("ZPOPMAX".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let count = if args.len() == 2 {
            Some(
                extract_string(&args[1])?
                    .parse::<usize>()
                    .map_err(|_| SpinelDBError::NotAnInteger)?,
            )
        } else {
            None
        };
        Ok(ZPopMax {
            pop_cmd: ZPop::new(key, PopSide::Max, count),
        })
    }
}

#[async_trait]
impl ExecutableCommand for ZPopMax {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        self.pop_cmd.execute(ctx).await
    }
}

impl CommandSpec for ZPopMax {
    fn name(&self) -> &'static str {
        "zpopmax"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        vec![self.pop_cmd.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.pop_cmd.key.clone()];
        if let Some(c) = self.pop_cmd.count {
            args.push(c.to_string().into());
        }
        args
    }
}
