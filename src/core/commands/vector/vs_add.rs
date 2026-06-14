// src/core/commands/vector/vs_add.rs

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

/// Implements the `VS.ADD` command to add a vector to an index.
///
/// VS.ADD key id vector [METADATA metadata]
#[derive(Debug, Clone, Default)]
pub struct VsAdd {
    pub key: Bytes,
    pub id: Bytes,
    pub vector: Vec<f32>,
    pub metadata: Option<Bytes>,
}

impl ParseCommand for VsAdd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("VS.ADD".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let id = extract_bytes(&args[1])?;

        // Parse vector values until we hit METADATA or end
        let mut vector = Vec::new();
        let mut i = 2;
        let mut metadata = None;

        while i < args.len() {
            let arg_str = extract_bytes(&args[i])?;
            if arg_str.eq_ignore_ascii_case(b"METADATA") {
                i += 1;
                if i < args.len() {
                    metadata = Some(extract_bytes(&args[i])?);
                }
                break;
            }
            let val_str = std::str::from_utf8(&arg_str).map_err(|_| {
                SpinelDBError::InvalidRequest("vector values must be valid UTF-8".to_string())
            })?;
            let val = val_str
                .parse::<f32>()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            vector.push(val);
            i += 1;
        }

        if vector.is_empty() {
            return Err(SpinelDBError::InvalidRequest(
                "vector must have at least one dimension".to_string(),
            ));
        }

        Ok(VsAdd {
            key,
            id,
            vector,
            metadata,
        })
    }
}

#[async_trait]
impl ExecutableCommand for VsAdd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            sv.add(self.id.clone(), self.vector.clone(), self.metadata.clone())
                .map_err(SpinelDBError::InvalidRequest)?;
            entry.version = entry.version.wrapping_add(1);
            Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsAdd {
    fn name(&self) -> &'static str {
        "vs.add"
    }
    fn arity(&self) -> i64 {
        -4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
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
        let mut args = vec![self.key.clone(), self.id.clone()];
        for &v in &self.vector {
            args.push(Bytes::from(v.to_string()));
        }
        if let Some(ref meta) = self.metadata {
            args.push(Bytes::from_static(b"METADATA"));
            args.push(meta.clone());
        }
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
    fn test_vsadd_parse_valid() {
        let c = VsAdd::parse(&[bs("idx"), bs("v1"), bs("1.0"), bs("2.0"), bs("3.0")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.id, Bytes::from_static(b"v1"));
        assert_eq!(c.vector, vec![1.0, 2.0, 3.0]);
        assert!(c.metadata.is_none());
    }

    #[test]
    fn test_vsadd_parse_with_metadata() {
        let c = VsAdd::parse(&[
            bs("idx"),
            bs("v1"),
            bs("1.0"),
            bs("2.0"),
            bs("METADATA"),
            bs("hello"),
        ])
        .unwrap();
        assert_eq!(c.metadata, Some(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_vsadd_parse_too_few_args() {
        let r = VsAdd::parse(&[bs("idx"), bs("v1")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsadd_parse_empty_vector() {
        let r = VsAdd::parse(&[bs("idx"), bs("v1"), bs("METADATA"), bs("x")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_vsadd_to_resp_args_round_trip() {
        let c = VsAdd::parse(&[bs("idx"), bs("v1"), bs("1.0"), bs("2.0")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[0], Bytes::from_static(b"idx"));
        assert_eq!(args[1], Bytes::from_static(b"v1"));
    }
}
