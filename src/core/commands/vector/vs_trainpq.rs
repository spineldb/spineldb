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

/// Implements the `VS.TRAINPQ` command: train Product Quantization codebooks.
///
/// VS.TRAINPQ key [SUBSPACES n] [BITS b]
#[derive(Debug, Clone, Default)]
pub struct VsTrainPq {
    pub key: Bytes,
    pub subspaces: usize,
    pub bits: usize,
}

impl ParseCommand for VsTrainPq {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("VS.TRAINPQ".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let mut subspaces = 0;
        let mut bits = 0;
        let mut i = 1;

        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            match upper.as_str() {
                "SUBSPACES" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.TRAINPQ".to_string()));
                    }
                    subspaces = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    i += 1;
                }
                "BITS" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.TRAINPQ".to_string()));
                    }
                    bits = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    i += 1;
                }
                _ => {
                    return Err(SpinelDBError::SyntaxError);
                }
            }
        }

        Ok(VsTrainPq {
            key,
            subspaces,
            bits,
        })
    }
}

#[async_trait]
impl ExecutableCommand for VsTrainPq {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            sv.train_pq(self.subspaces, self.bits)
                .map_err(SpinelDBError::InvalidRequest)?;

            let (subspaces, bits) = sv.pq_info();
            let pq_info = vec![
                RespValue::BulkString(Bytes::from_static(b"subspaces")),
                RespValue::BulkString(Bytes::from(subspaces.to_string())),
                RespValue::BulkString(Bytes::from_static(b"bits")),
                RespValue::BulkString(Bytes::from(bits.to_string())),
            ];

            Ok((RespValue::Array(pq_info), WriteOutcome::DidNotWrite))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsTrainPq {
    fn name(&self) -> &'static str {
        "vs.trainpq"
    }
    fn arity(&self) -> i64 {
        -2
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
        let mut args = vec![self.key.clone()];
        if self.subspaces > 0 {
            args.push(Bytes::from_static(b"SUBSPACES"));
            args.push(Bytes::from(self.subspaces.to_string()));
        }
        if self.bits > 0 {
            args.push(Bytes::from_static(b"BITS"));
            args.push(Bytes::from(self.bits.to_string()));
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
    fn test_parse_trainpq_basic() {
        let c = VsTrainPq::parse(&[bs("idx")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.subspaces, 0);
        assert_eq!(c.bits, 0);
    }

    #[test]
    fn test_parse_trainpq_with_params() {
        let c =
            VsTrainPq::parse(&[bs("idx"), bs("SUBSPACES"), bs("8"), bs("BITS"), bs("8")]).unwrap();
        assert_eq!(c.subspaces, 8);
        assert_eq!(c.bits, 8);
    }

    #[test]
    fn test_parse_trainpq_no_args_is_error() {
        let r = VsTrainPq::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_parse_trainpq_invalid_keyword() {
        let r = VsTrainPq::parse(&[bs("idx"), bs("FOO"), bs("1")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }
}
