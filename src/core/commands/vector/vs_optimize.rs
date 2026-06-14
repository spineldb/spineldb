// src/core/commands/vector/vs_optimize.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.OPTIMIZE` command to compact and optimize the index.
///
/// VS.OPTIMIZE key
#[derive(Debug, Clone, Default)]
pub struct VsOptimize {
    pub key: Bytes,
}

impl ParseCommand for VsOptimize {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 1 {
            return Err(SpinelDBError::WrongArgumentCount("VS.OPTIMIZE".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        Ok(VsOptimize { key })
    }
}

#[async_trait]
impl ExecutableCommand for VsOptimize {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            let removed = sv.optimize();
            entry.version = entry.version.wrapping_add(1);

            Ok((
                RespValue::Integer(removed as i64),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsOptimize {
    fn name(&self) -> &'static str {
        "vs.optimize"
    }
    fn arity(&self) -> i64 {
        2
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
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_vsoptimize_parse_valid() {
        let c = VsOptimize::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
    }

    #[test]
    fn test_vsoptimize_too_few_args() {
        let r = VsOptimize::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsoptimize_too_many_args() {
        let r = VsOptimize::parse(&[bs("idx"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsoptimize_to_resp_args() {
        let c = VsOptimize::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.to_resp_args(), vec![Bytes::from_static(b"idx")]);
    }
}
