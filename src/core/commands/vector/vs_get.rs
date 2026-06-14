// src/core/commands/vector/vs_get.rs

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

/// Implements the `VS.GET` command to retrieve a vector by ID.
///
/// VS.GET key id
#[derive(Debug, Clone, Default)]
pub struct VsGet {
    pub key: Bytes,
    pub id: Bytes,
}

impl ParseCommand for VsGet {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 2 {
            return Err(SpinelDBError::WrongArgumentCount("VS.GET".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let id = extract_bytes(&args[1])?;

        Ok(VsGet { key, id })
    }
}

#[async_trait]
impl ExecutableCommand for VsGet {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref sv) = entry.data {
            match sv.get(&self.id) {
                Some(vec_entry) => {
                    let mut result = Vec::new();
                    result.push(RespValue::BulkString(vec_entry.id.clone()));
                    let vector_vals: Vec<RespValue> = vec_entry
                        .vector
                        .iter()
                        .map(|v| RespValue::BulkString(Bytes::from(v.to_string())))
                        .collect();
                    result.push(RespValue::Array(vector_vals));
                    if let Some(ref meta) = vec_entry.metadata {
                        result.push(RespValue::BulkString(meta.clone()));
                    } else {
                        result.push(RespValue::Null);
                    }
                    Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
                }
                None => Ok((RespValue::Null, WriteOutcome::DidNotWrite)),
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsGet {
    fn name(&self) -> &'static str {
        "vs.get"
    }
    fn arity(&self) -> i64 {
        3
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
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![self.key.clone(), self.id.clone()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_vsget_parse_valid() {
        let c = VsGet::parse(&[bs("idx"), bs("v1")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.id, Bytes::from_static(b"v1"));
    }

    #[test]
    fn test_vsget_too_few_args() {
        let r = VsGet::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsget_too_many_args() {
        let r = VsGet::parse(&[bs("idx"), bs("v1"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsget_to_resp_args() {
        let c = VsGet::parse(&[bs("idx"), bs("v1")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(
            args,
            vec![Bytes::from_static(b"idx"), Bytes::from_static(b"v1")]
        );
    }
}
