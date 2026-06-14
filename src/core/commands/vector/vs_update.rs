// src/core/commands/vector/vs_update.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.UPDATE` command for in-place vector/metadata update.
///
/// VS.UPDATE key id [VECTOR v1 v2 ...] [METADATA metadata]
#[derive(Debug, Clone, Default)]
pub struct VsUpdate {
    pub key: Bytes,
    pub id: Bytes,
    pub vector: Option<Vec<f32>>,
    pub metadata: Option<Option<Bytes>>,
}

impl ParseCommand for VsUpdate {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("VS.UPDATE".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let id = extract_bytes(&args[1])?;

        let mut vector = None;
        let mut metadata = None;
        let mut i = 2;

        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            if upper == "VECTOR" {
                i += 1;
                let mut vec_vals = Vec::new();
                while i < args.len() {
                    let val_str = extract_string(&args[i])?;
                    // Stop if we hit another keyword
                    if val_str.to_uppercase() == "METADATA" {
                        break;
                    }
                    let val = val_str
                        .parse::<f32>()
                        .map_err(|_| SpinelDBError::NotAFloat)?;
                    vec_vals.push(val);
                    i += 1;
                }
                if vec_vals.is_empty() {
                    return Err(SpinelDBError::InvalidRequest(
                        "VECTOR requires at least one value".to_string(),
                    ));
                }
                vector = Some(vec_vals);
                continue;
            } else if upper == "METADATA" {
                i += 1;
                if i >= args.len() {
                    return Err(SpinelDBError::WrongArgumentCount("VS.UPDATE".to_string()));
                }
                metadata = Some(Some(extract_bytes(&args[i])?));
                i += 1;
                continue;
            }

            i += 1;
        }

        if vector.is_none() && metadata.is_none() {
            return Err(SpinelDBError::InvalidRequest(
                "VS.UPDATE requires at least one of VECTOR or METADATA".to_string(),
            ));
        }

        Ok(VsUpdate {
            key,
            id,
            vector,
            metadata,
        })
    }
}

#[async_trait]
impl ExecutableCommand for VsUpdate {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            sv.update(&self.id, self.vector.clone(), self.metadata.clone())
                .map_err(SpinelDBError::InvalidRequest)?;

            Ok((
                RespValue::SimpleString("OK".to_string()),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsUpdate {
    fn name(&self) -> &'static str {
        "vs.update"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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
        if let Some(ref vector) = self.vector {
            args.push(Bytes::from_static(b"VECTOR"));
            for &v in vector {
                args.push(Bytes::from(v.to_string()));
            }
        }
        if let Some(ref meta) = self.metadata {
            args.push(Bytes::from_static(b"METADATA"));
            if let Some(m) = meta {
                args.push(m.clone());
            }
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
    fn test_vsupdate_parse_vector_only() {
        let c = VsUpdate::parse(&[
            bs("idx"),
            bs("v1"),
            bs("VECTOR"),
            bs("1.0"),
            bs("2.0"),
            bs("3.0"),
        ])
        .unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.id, Bytes::from_static(b"v1"));
        assert_eq!(c.vector, Some(vec![1.0, 2.0, 3.0]));
        assert!(c.metadata.is_none());
    }

    #[test]
    fn test_vsupdate_parse_metadata_only() {
        let c =
            VsUpdate::parse(&[bs("idx"), bs("v1"), bs("METADATA"), bs(r#"{"type":"a"}"#)]).unwrap();
        assert!(c.vector.is_none());
        assert_eq!(c.metadata, Some(Some(Bytes::from(r#"{"type":"a"}"#))));
    }

    #[test]
    fn test_vsupdate_parse_both() {
        let c = VsUpdate::parse(&[
            bs("idx"),
            bs("v1"),
            bs("VECTOR"),
            bs("1.0"),
            bs("2.0"),
            bs("METADATA"),
            bs("new_meta"),
        ])
        .unwrap();
        assert_eq!(c.vector, Some(vec![1.0, 2.0]));
        assert_eq!(c.metadata, Some(Some(Bytes::from("new_meta"))));
    }

    #[test]
    fn test_vsupdate_too_few_args() {
        let r = VsUpdate::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsupdate_no_vector_or_metadata() {
        let r = VsUpdate::parse(&[bs("idx"), bs("v1")]);
        assert!(r.is_err());
    }
}
