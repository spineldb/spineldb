// src/core/commands/vector/vs_quantize.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::vector::QuantizationMethod;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.QUANTIZE` command to enable quantization for memory efficiency.
///
/// VS.QUANTIZE key method
/// method: INT8 or NONE
#[derive(Debug, Clone, Default)]
pub struct VsQuantize {
    pub key: Bytes,
    pub method: QuantizationMethod,
}

impl ParseCommand for VsQuantize {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() != 2 {
            return Err(SpinelDBError::WrongArgumentCount("VS.QUANTIZE".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let method_str = extract_string(&args[1])?;
        let method = QuantizationMethod::parse(&method_str).ok_or_else(|| {
            SpinelDBError::InvalidRequest(format!(
                "invalid quantization method '{}', expected INT8 or NONE",
                method_str
            ))
        })?;

        Ok(VsQuantize { key, method })
    }
}

#[async_trait]
impl ExecutableCommand for VsQuantize {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            let saved = sv
                .enable_quantization(self.method)
                .map_err(SpinelDBError::InvalidRequest)?;
            entry.version = entry.version.wrapping_add(1);

            Ok((
                RespValue::Integer(saved as i64),
                WriteOutcome::Write { keys_modified: 1 },
            ))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsQuantize {
    fn name(&self) -> &'static str {
        "vs.quantize"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![
            self.key.clone(),
            Bytes::from(self.method.as_str().to_string()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_vsquantize_parse_valid() {
        let c = VsQuantize::parse(&[bs("idx"), bs("INT8")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.method, QuantizationMethod::Int8);
    }

    #[test]
    fn test_vsquantize_parse_none() {
        let c = VsQuantize::parse(&[bs("idx"), bs("NONE")]).unwrap();
        assert_eq!(c.method, QuantizationMethod::None);
    }

    #[test]
    fn test_vsquantize_too_few_args() {
        let r = VsQuantize::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vsquantize_invalid_method() {
        let r = VsQuantize::parse(&[bs("idx"), bs("INVALID")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_vsquantize_to_resp_args() {
        let c = VsQuantize::parse(&[bs("idx"), bs("INT8")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"idx"));
        assert_eq!(args[1], Bytes::from_static(b"INT8"));
    }
}
