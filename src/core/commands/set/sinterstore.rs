// src/core/commands/set/sinterstore.rs

use super::set_ops_logic::{execute_sinter, store_set_result};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct SInterStore {
    pub destination: Bytes,
    pub keys: Vec<Bytes>,
}

impl ParseCommand for SInterStore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("SINTERSTORE".to_string()));
        }
        let destination = extract_bytes(&args[0])?;
        let keys = args[1..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;
        Ok(SInterStore { destination, keys })
    }
}

#[async_trait]
impl ExecutableCommand for SInterStore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let intersection_set = execute_sinter(&self.keys, ctx).await?;
        store_set_result(&self.destination, intersection_set, ctx)
    }
}

impl CommandSpec for SInterStore {
    fn name(&self) -> &'static str {
        "sinterstore"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        let mut all_keys = vec![self.destination.clone()];
        all_keys.extend_from_slice(&self.keys);
        all_keys
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut all_args = vec![self.destination.clone()];
        all_args.extend_from_slice(&self.keys);
        all_args
    }
}
