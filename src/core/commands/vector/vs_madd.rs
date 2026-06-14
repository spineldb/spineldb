// src/core/commands/vector/vs_madd.rs

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

/// Implements the `VS.MADD` command to add multiple vectors at once.
///
/// VS.MADD key [id vector [METADATA metadata] ...]
#[derive(Debug, Clone, Default)]
pub struct VsMAdd {
    pub key: Bytes,
    pub entries: Vec<MAddEntry>,
}

#[derive(Debug, Clone)]
pub struct MAddEntry {
    pub id: Bytes,
    pub vector: Vec<f32>,
    pub metadata: Option<Bytes>,
}

impl ParseCommand for VsMAdd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("VS.MADD".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let mut entries = Vec::new();
        let mut i = 1;

        while i < args.len() {
            // Parse id
            let id = extract_bytes(&args[i])?;
            i += 1;

            // Parse vector values until we hit METADATA, next id, or end
            let mut vector = Vec::new();
            let mut metadata = None;

            while i < args.len() {
                let arg_bytes = extract_bytes(&args[i])?;
                if arg_bytes.eq_ignore_ascii_case(b"METADATA") {
                    i += 1;
                    if i < args.len() {
                        metadata = Some(extract_bytes(&args[i])?);
                        i += 1;
                    }
                    break;
                }
                // Try to parse as float - if it fails, this might be the next id
                let val_str = std::str::from_utf8(&arg_bytes).map_err(|_| {
                    SpinelDBError::InvalidRequest("vector values must be valid UTF-8".to_string())
                })?;
                if let Ok(val) = val_str.parse::<f32>() {
                    vector.push(val);
                    i += 1;
                } else {
                    // Not a float, this is the next entry's id
                    break;
                }
            }

            if vector.is_empty() {
                return Err(SpinelDBError::InvalidRequest(
                    "vector must have at least one dimension".to_string(),
                ));
            }

            entries.push(MAddEntry {
                id,
                vector,
                metadata,
            });
        }

        if entries.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("VS.MADD".to_string()));
        }

        Ok(VsMAdd { key, entries })
    }
}

#[async_trait]
impl ExecutableCommand for VsMAdd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            let mut results = Vec::new();
            let mut any_modified = false;

            for madd_entry in &self.entries {
                match sv.add(
                    madd_entry.id.clone(),
                    madd_entry.vector.clone(),
                    madd_entry.metadata.clone(),
                ) {
                    Ok(is_new) => {
                        results.push(RespValue::SimpleString("OK".into()));
                        if is_new {
                            any_modified = true;
                        }
                    }
                    Err(e) => {
                        results.push(RespValue::Error(e));
                    }
                }
            }

            if any_modified {
                entry.version = entry.version.wrapping_add(1);
            }

            Ok((
                RespValue::Array(results),
                WriteOutcome::Write {
                    keys_modified: if any_modified { 1 } else { 0 },
                },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsMAdd {
    fn name(&self) -> &'static str {
        "vs.madd"
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
        for entry in &self.entries {
            args.push(entry.id.clone());
            for &v in &entry.vector {
                args.push(Bytes::from(v.to_string()));
            }
            if let Some(ref meta) = entry.metadata {
                args.push(Bytes::from_static(b"METADATA"));
                args.push(meta.clone());
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
    fn test_vsmadd_parse_valid() {
        let c = VsMAdd::parse(&[
            bs("idx"),
            bs("v1"),
            bs("1.0"),
            bs("2.0"),
            bs("v2"),
            bs("3.0"),
            bs("4.0"),
        ])
        .unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.entries.len(), 2);
        assert_eq!(c.entries[0].id, Bytes::from_static(b"v1"));
        assert_eq!(c.entries[0].vector, vec![1.0, 2.0]);
        assert_eq!(c.entries[1].id, Bytes::from_static(b"v2"));
        assert_eq!(c.entries[1].vector, vec![3.0, 4.0]);
    }

    #[test]
    fn test_vsmadd_parse_no_entries() {
        let r = VsMAdd::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsmadd_to_resp_args() {
        let c = VsMAdd::parse(&[
            bs("idx"),
            bs("v1"),
            bs("1.0"),
            bs("2.0"),
            bs("v2"),
            bs("3.0"),
            bs("4.0"),
        ])
        .unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"idx"));
        assert_eq!(args.len(), 7); // key + v1 + 2 vals + v2 + 2 vals
    }
}
