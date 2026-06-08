// src/core/commands/string/psetex.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::commands::string::set::{Set, SetCondition, TtlOption};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct PSetEx {
    pub key: Bytes,
    pub milliseconds: u64,
    pub value: Bytes,
}

impl ParseCommand for PSetEx {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 3, "PSETEX")?;
        let milliseconds = extract_string(&args[1])?.parse::<u64>()?;
        if milliseconds == 0 {
            return Err(SpinelDBError::InvalidState(
                "invalid expire time in PSETEX".into(),
            ));
        }
        Ok(PSetEx {
            key: extract_bytes(&args[0])?,
            milliseconds,
            value: extract_bytes(&args[2])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for PSetEx {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Delegasikan eksekusi ke perintah Set yang lebih generik.
        let set_cmd = Set {
            key: self.key.clone(),
            value: self.value.clone(),
            ttl: TtlOption::Milliseconds(self.milliseconds),
            condition: SetCondition::None,
            get: false,
        };
        set_cmd.execute(ctx).await
    }
}

impl CommandSpec for PSetEx {
    fn name(&self) -> &'static str {
        "psetex"
    }
    fn arity(&self) -> i64 {
        4
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
        vec![
            self.key.clone(),
            self.milliseconds.to_string().into(),
            self.value.clone(),
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
    fn test_psetex_parse_valid() {
        let c = PSetEx::parse(&[bs("k"), bs("100"), bs("v")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.milliseconds, 100);
        assert_eq!(c.value, Bytes::from_static(b"v"));
    }

    #[test]
    fn test_psetex_parse_zero_milliseconds_is_error() {
        let r = PSetEx::parse(&[bs("k"), bs("0"), bs("v")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidState(_))));
    }

    #[test]
    fn test_psetex_parse_too_few_args_is_error() {
        let r = PSetEx::parse(&[bs("k"), bs("100")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_psetex_parse_too_many_args_is_error() {
        let r = PSetEx::parse(&[bs("k"), bs("100"), bs("v"), bs("extra")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_psetex_parse_non_numeric_ms_is_error() {
        let r = PSetEx::parse(&[bs("k"), bs("soon"), bs("v")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_psetex_to_resp_args() {
        let c = PSetEx {
            key: Bytes::from_static(b"key"),
            milliseconds: 500,
            value: Bytes::from_static(b"val"),
        };
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[1], Bytes::from_static(b"500"));
    }

    #[test]
    fn test_psetex_spec() {
        let c = PSetEx {
            ..Default::default()
        };
        assert_eq!(c.name(), "psetex");
        assert_eq!(c.arity(), 4);
        assert!(c.flags().contains(CommandFlags::WRITE));
        assert_eq!(c.first_key(), 1);
    }
}
