// src/core/commands/generic/restore.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};

use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct Restore {
    pub key: Bytes,
    pub ttl_ms: u64,
    pub serialized_value: Bytes,
    pub replace: bool,
}

impl ParseCommand for Restore {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("RESTORE".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let ttl_ms = extract_string(&args[1])?.parse()?;
        let serialized_value = extract_bytes(&args[2])?;

        let mut replace = false;
        if args.len() > 3 {
            if extract_string(&args[3])?.eq_ignore_ascii_case("replace") {
                replace = true;
            } else {
                return Err(SpinelDBError::SyntaxError);
            }
        }

        Ok(Restore {
            key,
            ttl_ms,
            serialized_value,
            replace,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Restore {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        if !self.replace && guard.peek(&self.key).is_some_and(|e| !e.is_expired()) {
            return Err(SpinelDBError::InvalidState(
                "BUSYKEY Target key name already exists.".to_string(),
            ));
        }

        // Use the SPLDB parser to convert bytes back into a StoredValue
        let mut value_to_restore =
            crate::core::persistence::spldb::deserialize_value(&self.serialized_value)?;

        // Set TTL if provided
        if self.ttl_ms > 0 {
            value_to_restore.expiry = Some(Instant::now() + Duration::from_millis(self.ttl_ms));
        }

        guard.put(self.key.clone(), value_to_restore);

        Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for Restore {
    fn name(&self) -> &'static str {
        "restore"
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
        let mut args = vec![
            self.key.clone(),
            self.ttl_ms.to_string().into(),
            self.serialized_value.clone(),
        ];
        if self.replace {
            args.push("REPLACE".into());
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::commands::command_spec::CommandSpec;
    use crate::core::commands::command_trait::ParseCommand;
    use crate::core::protocol::RespFrame;

    fn bulk(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::from(s.to_owned()))
    }

    fn bulk_bytes(b: &[u8]) -> RespFrame {
        RespFrame::BulkString(Bytes::from(b.to_vec()))
    }

    #[test]
    fn test_parse_too_few_args_errors() {
        assert!(Restore::parse(&[]).is_err());
        assert!(Restore::parse(&[bulk("key")]).is_err());
        assert!(Restore::parse(&[bulk("key"), bulk("0")]).is_err());
    }

    #[test]
    fn test_parse_minimal() {
        let cmd = Restore::parse(&[bulk("mykey"), bulk("0"), bulk_bytes(b"serialized")]).unwrap();
        assert_eq!(cmd.key, Bytes::from_static(b"mykey"));
        assert_eq!(cmd.ttl_ms, 0);
        assert_eq!(cmd.serialized_value.as_ref(), b"serialized");
        assert!(!cmd.replace);
    }

    #[test]
    fn test_parse_with_ttl() {
        let cmd = Restore::parse(&[bulk("mykey"), bulk("5000"), bulk_bytes(b"data")]).unwrap();
        assert_eq!(cmd.ttl_ms, 5000);
    }

    #[test]
    fn test_parse_with_replace() {
        let cmd = Restore::parse(&[
            bulk("mykey"),
            bulk("0"),
            bulk_bytes(b"data"),
            bulk("REPLACE"),
        ])
        .unwrap();
        assert!(cmd.replace);
    }

    #[test]
    fn test_parse_replace_case_insensitive() {
        let cmd = Restore::parse(&[
            bulk("mykey"),
            bulk("0"),
            bulk_bytes(b"data"),
            bulk("replace"),
        ])
        .unwrap();
        assert!(cmd.replace);
    }

    #[test]
    fn test_parse_replace_mixed_case() {
        let cmd = Restore::parse(&[
            bulk("mykey"),
            bulk("0"),
            bulk_bytes(b"data"),
            bulk("Replace"),
        ])
        .unwrap();
        assert!(cmd.replace);
    }

    #[test]
    fn test_parse_unknown_option_errors() {
        let cmd = Restore::parse(&[
            bulk("mykey"),
            bulk("0"),
            bulk_bytes(b"data"),
            bulk("INVALID"),
        ]);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_parse_bad_ttl_errors() {
        let cmd = Restore::parse(&[bulk("mykey"), bulk("notanumber"), bulk_bytes(b"data")]);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Restore::default().name(), "restore");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Restore::default().arity(), -4);
    }

    #[test]
    fn test_command_flags() {
        let flags = Restore::default().flags();
        assert!(flags.contains(CommandFlags::WRITE));
        assert!(flags.contains(CommandFlags::DENY_OOM));
        assert!(flags.contains(CommandFlags::MOVABLEKEYS));
    }

    #[test]
    fn test_get_keys() {
        let cmd = Restore {
            key: Bytes::from_static(b"k"),
            ..Default::default()
        };
        assert_eq!(cmd.get_keys(), vec![Bytes::from_static(b"k")]);
    }

    #[test]
    fn test_to_resp_args_no_replace() {
        let cmd = Restore {
            key: Bytes::from_static(b"k"),
            ttl_ms: 100,
            serialized_value: Bytes::from_static(b"sv"),
            replace: false,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"k"));
        assert_eq!(args[1], Bytes::from_static(b"100"));
        assert_eq!(args[2], Bytes::from_static(b"sv"));
    }

    #[test]
    fn test_to_resp_args_with_replace() {
        let cmd = Restore {
            key: Bytes::from_static(b"k"),
            ttl_ms: 0,
            serialized_value: Bytes::from_static(b"sv"),
            replace: true,
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[3], Bytes::from_static(b"REPLACE"));
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Restore::default();
        assert_eq!(cmd.first_key(), 1);
        assert_eq!(cmd.last_key(), 1);
        assert_eq!(cmd.step(), 1);
    }
}
