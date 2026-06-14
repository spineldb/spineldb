// src/core/commands/vector/vs_del.rs

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

/// Implements the `VS.DEL` command to remove vectors by ID.
///
/// VS.DEL key id [id ...]
#[derive(Debug, Clone, Default)]
pub struct VsDel {
    pub key: Bytes,
    pub ids: Vec<Bytes>,
}

impl ParseCommand for VsDel {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("VS.DEL".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let mut ids = Vec::new();
        for arg in &args[1..] {
            ids.push(extract_bytes(arg)?);
        }

        Ok(VsDel { key, ids })
    }
}

#[async_trait]
impl ExecutableCommand for VsDel {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            let mut count = 0;
            for id in &self.ids {
                if sv.del(id) {
                    count += 1;
                }
            }

            if count > 0 {
                entry.version = entry.version.wrapping_add(1);
            }

            Ok((
                RespValue::Integer(count as i64),
                WriteOutcome::Write {
                    keys_modified: if count > 0 { 1 } else { 0 },
                },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsDel {
    fn name(&self) -> &'static str {
        "vs.del"
    }
    fn arity(&self) -> i64 {
        -3
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
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.key.clone()];
        args.extend_from_slice(&self.ids);
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_vsdel_parse_valid() {
        let c = VsDel::parse(&[bs("idx"), bs("v1")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.ids, vec![Bytes::from_static(b"v1")]);
    }

    #[test]
    fn test_vsdel_parse_multiple() {
        let c = VsDel::parse(&[bs("idx"), bs("v1"), bs("v2"), bs("v3")]).unwrap();
        assert_eq!(c.ids.len(), 3);
    }

    #[test]
    fn test_vsdel_too_few_args() {
        let r = VsDel::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsdel_to_resp_args() {
        let c = VsDel::parse(&[bs("idx"), bs("v1"), bs("v2")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(
            args,
            vec![
                Bytes::from_static(b"idx"),
                Bytes::from_static(b"v1"),
                Bytes::from_static(b"v2")
            ]
        );
    }
}
