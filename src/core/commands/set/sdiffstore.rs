// src/core/commands/set/sdiffstore.rs
use super::set_ops_logic::{execute_sdiff, store_set_result};
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct SdiffStore {
    pub destination: Bytes,
    pub keys: Vec<Bytes>,
}

impl ParseCommand for SdiffStore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("SDIFFSTORE".to_string()));
        }
        let destination = extract_bytes(&args[0])?;
        let keys = args[1..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;
        Ok(SdiffStore { destination, keys })
    }
}

#[async_trait]
impl ExecutableCommand for SdiffStore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let diff_set = execute_sdiff(&self.keys, ctx).await?;
        store_set_result(&self.destination, diff_set, ctx)
    }
}

impl CommandSpec for SdiffStore {
    fn name(&self) -> &'static str {
        "sdiffstore"
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_sdiffstore_parses_destination_and_keys() {
        let c = SdiffStore::parse(&[bs("dst"), bs("k1"), bs("k2")]).unwrap();
        assert_eq!(c.destination, Bytes::from_static(b"dst"));
        assert_eq!(c.keys.len(), 2);
    }

    #[test]
    fn test_sdiffstore_with_too_few_args_is_error() {
        let r = SdiffStore::parse(&[bs("dst")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_sdiffstore_with_non_bulk_dest_is_wrong_type() {
        let r = SdiffStore::parse(&[RespFrame::Integer(1), bs("k1")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_sdiffstore_to_resp_args_round_trips() {
        let c = SdiffStore::parse(&[bs("dst"), bs("a"), bs("b")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
    }
}
