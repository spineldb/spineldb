// src/core/commands/script.rs

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

#[derive(Debug, Clone, Default)]
pub enum ScriptSubcommand {
    #[default]
    Flush,
    Exists(Vec<String>),
    Load(Bytes),
}

#[derive(Debug, Clone, Default)]
pub struct Script {
    pub subcommand: ScriptSubcommand,
}

impl ParseCommand for Script {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SCRIPT".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "flush" => {
                if args.len() != 1 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "SCRIPT FLUSH".to_string(),
                    ));
                }
                ScriptSubcommand::Flush
            }
            "exists" => {
                if args.len() < 2 {
                    return Err(SpinelDBError::WrongArgumentCount(
                        "SCRIPT EXISTS".to_string(),
                    ));
                }
                let sha1s = args[1..]
                    .iter()
                    .map(extract_string)
                    .collect::<Result<_, _>>()?;
                ScriptSubcommand::Exists(sha1s)
            }
            "load" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount("SCRIPT LOAD".to_string()));
                }
                let script_body = extract_bytes(&args[1])?;
                ScriptSubcommand::Load(script_body)
            }
            _ => return Err(SpinelDBError::UnknownCommand(format!("SCRIPT {sub_str}"))),
        };

        Ok(Script { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Script {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            ScriptSubcommand::Flush => {
                ctx.state.scripting.flush();
                Ok((
                    RespValue::SimpleString("OK".into()),
                    WriteOutcome::Write { keys_modified: 0 },
                ))
            }
            ScriptSubcommand::Exists(sha1s) => {
                let results = ctx.state.scripting.exists(sha1s);
                let resp_values = results.into_iter().map(RespValue::Integer).collect();
                Ok((RespValue::Array(resp_values), WriteOutcome::DidNotWrite))
            }
            ScriptSubcommand::Load(script) => {
                let sha1 = ctx.state.scripting.load(script.clone());
                Ok((
                    RespValue::BulkString(sha1.into()),
                    WriteOutcome::DidNotWrite,
                ))
            }
        }
    }
}

impl CommandSpec for Script {
    fn name(&self) -> &'static str {
        "script"
    }

    fn arity(&self) -> i64 {
        -2
    }

    fn flags(&self) -> CommandFlags {
        match &self.subcommand {
            // FLUSH and LOAD are write operations that must be replicated.
            ScriptSubcommand::Flush | ScriptSubcommand::Load(_) => {
                CommandFlags::ADMIN | CommandFlags::WRITE
            }
            // EXISTS is a read operation and does not need to be replicated.
            ScriptSubcommand::Exists(_) => CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE,
        }
    }

    fn first_key(&self) -> i64 {
        0
    }

    fn last_key(&self) -> i64 {
        0
    }

    fn step(&self) -> i64 {
        0
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        match &self.subcommand {
            ScriptSubcommand::Flush => vec!["FLUSH".into()],
            ScriptSubcommand::Exists(sha1s) => {
                let mut args = vec!["EXISTS".into()];
                args.extend(sha1s.iter().map(|s| s.clone().into()));
                args
            }
            ScriptSubcommand::Load(script) => vec!["LOAD".into(), script.clone()],
        }
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
    fn test_parse_empty_errors() {
        assert!(Script::parse(&[]).is_err());
    }

    #[test]
    fn test_parse_flush() {
        let cmd = Script::parse(&[bulk("FLUSH")]).unwrap();
        assert!(matches!(cmd.subcommand, ScriptSubcommand::Flush));
    }

    #[test]
    fn test_parse_flush_case_insensitive() {
        let cmd = Script::parse(&[bulk("flush")]).unwrap();
        assert!(matches!(cmd.subcommand, ScriptSubcommand::Flush));
    }

    #[test]
    fn test_parse_flush_extra_args_errors() {
        assert!(Script::parse(&[bulk("FLUSH"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_exists_no_sha1_errors() {
        assert!(Script::parse(&[bulk("EXISTS")]).is_err());
    }

    #[test]
    fn test_parse_exists_one_sha1() {
        let cmd = Script::parse(&[bulk("EXISTS"), bulk("abc123")]).unwrap();
        match &cmd.subcommand {
            ScriptSubcommand::Exists(sha1s) => {
                assert_eq!(sha1s.len(), 1);
                assert_eq!(sha1s[0], "abc123");
            }
            _ => panic!("expected Exists"),
        }
    }

    #[test]
    fn test_parse_exists_multiple_sha1s() {
        let cmd =
            Script::parse(&[bulk("EXISTS"), bulk("sha1a"), bulk("sha1b"), bulk("sha1c")]).unwrap();
        match &cmd.subcommand {
            ScriptSubcommand::Exists(sha1s) => {
                assert_eq!(sha1s.len(), 3);
            }
            _ => panic!("expected Exists"),
        }
    }

    #[test]
    fn test_parse_load() {
        let cmd = Script::parse(&[bulk("LOAD"), bulk_bytes(b"return 1")]).unwrap();
        match &cmd.subcommand {
            ScriptSubcommand::Load(script) => {
                assert_eq!(script.as_ref(), b"return 1");
            }
            _ => panic!("expected Load"),
        }
    }

    #[test]
    fn test_parse_load_no_script_errors() {
        assert!(Script::parse(&[bulk("LOAD")]).is_err());
    }

    #[test]
    fn test_parse_load_extra_args_errors() {
        assert!(Script::parse(&[bulk("LOAD"), bulk("s"), bulk("extra")]).is_err());
    }

    #[test]
    fn test_parse_unknown_subcommand_errors() {
        assert!(Script::parse(&[bulk("UNKNOWN")]).is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Script::default().name(), "script");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(Script::default().arity(), -2);
    }

    #[test]
    fn test_flags_flush() {
        let cmd = Script {
            subcommand: ScriptSubcommand::Flush,
        };
        let flags = CommandSpec::flags(&cmd);
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::WRITE));
    }

    #[test]
    fn test_flags_load() {
        let cmd = Script {
            subcommand: ScriptSubcommand::Load(Bytes::from_static(b"s")),
        };
        let flags = CommandSpec::flags(&cmd);
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::WRITE));
    }

    #[test]
    fn test_flags_exists() {
        let cmd = Script {
            subcommand: ScriptSubcommand::Exists(vec!["sha".into()]),
        };
        let flags = CommandSpec::flags(&cmd);
        assert!(flags.contains(CommandFlags::ADMIN));
        assert!(flags.contains(CommandFlags::NO_PROPAGATE));
    }

    #[test]
    fn test_get_keys_empty() {
        assert!(Script::default().get_keys().is_empty());
    }

    #[test]
    fn test_first_last_step() {
        let cmd = Script::default();
        assert_eq!(cmd.first_key(), 0);
        assert_eq!(cmd.last_key(), 0);
        assert_eq!(cmd.step(), 0);
    }

    #[test]
    fn test_to_resp_args_flush() {
        let cmd = Script {
            subcommand: ScriptSubcommand::Flush,
        };
        assert_eq!(cmd.to_resp_args(), vec![Bytes::from_static(b"FLUSH")]);
    }

    #[test]
    fn test_to_resp_args_exists() {
        let cmd = Script {
            subcommand: ScriptSubcommand::Exists(vec!["s1".into(), "s2".into()]),
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"EXISTS"));
        assert_eq!(args[1], Bytes::from_static(b"s1"));
        assert_eq!(args[2], Bytes::from_static(b"s2"));
    }

    #[test]
    fn test_to_resp_args_load() {
        let cmd = Script {
            subcommand: ScriptSubcommand::Load(Bytes::from_static(b"body")),
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"LOAD"));
        assert_eq!(args[1], Bytes::from_static(b"body"));
    }
}
