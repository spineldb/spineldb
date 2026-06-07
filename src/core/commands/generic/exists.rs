// src/core/commands/generic/exists.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct Exists {
    pub keys: Vec<Bytes>,
}
impl ParseCommand for Exists {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("EXISTS".to_string()));
        }
        let keys = args
            .iter()
            .map(extract_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Exists { keys })
    }
}
#[async_trait]
impl ExecutableCommand for Exists {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut count = 0;
        let mut guards = match std::mem::replace(&mut ctx.locks, ExecutionLocks::None) {
            ExecutionLocks::Multi { guards } => guards,
            ExecutionLocks::Single { shard_index, guard } => {
                let mut map = BTreeMap::new();
                map.insert(shard_index, guard);
                map
            }
            _ => {
                return Err(SpinelDBError::Internal(
                    "EXISTS requires appropriate lock (Single or Multi)".into(),
                ));
            }
        };

        for key in &self.keys {
            let shard_index = ctx.db.get_shard_index(key);
            if let Some(guard) = guards.get_mut(&shard_index)
                && let Some(entry) = guard.peek(key)
                && !entry.is_expired()
            {
                count += 1;
            }
        }

        Ok((RespValue::Integer(count), WriteOutcome::DidNotWrite))
    }
}
impl CommandSpec for Exists {
    fn name(&self) -> &'static str {
        "exists"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_exists_with_single_key() {
        let e = Exists::parse(&[bs("k1")]).unwrap();
        assert_eq!(e.keys, vec![Bytes::from_static(b"k1")]);
    }

    #[test]
    fn test_exists_with_many_keys() {
        let e = Exists::parse(&[bs("a"), bs("b")]).unwrap();
        assert_eq!(e.keys.len(), 2);
    }

    #[test]
    fn test_exists_with_no_args_is_error() {
        let r = Exists::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_exists_with_non_bulk_is_wrong_type() {
        let r = Exists::parse(&[RespFrame::SimpleString("x".to_string())]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_exists_to_resp_args_round_trips() {
        let e = Exists::parse(&[bs("foo"), bs("bar")]).unwrap();
        let args = e.to_resp_args();
        assert_eq!(args, e.keys);
    }
}
