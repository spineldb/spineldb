// src/core/commands/generic/evalsha.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::generic::eval::Eval; // Reuse Eval's logic
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct EvalSha {
    pub sha1: String,
    pub num_keys: usize,
    pub keys: Vec<Bytes>,
    pub args: Vec<Bytes>,
}

impl ParseCommand for EvalSha {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("EVALSHA".to_string()));
        }
        let sha1 = extract_string(&args[0])?;
        let num_keys: usize = extract_string(&args[1])?.parse()?;

        if args.len() < 2 + num_keys {
            return Err(SpinelDBError::InvalidState(
                "Number of keys can't be greater than number of args".into(),
            ));
        }

        let keys = args[2..2 + num_keys]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;
        let eval_args = args[2 + num_keys..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        Ok(EvalSha {
            sha1,
            num_keys,
            keys,
            args: eval_args,
        })
    }
}

#[async_trait]
impl ExecutableCommand for EvalSha {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let script = ctx.state.scripting.get(&self.sha1).ok_or_else(|| {
            SpinelDBError::InvalidState("NOSCRIPT No matching script. Please use EVAL.".to_string())
        })?;

        let eval_cmd = Eval {
            script,
            num_keys: self.num_keys,
            keys: self.keys.clone(),
            args: self.args.clone(),
        };

        // Delegate execution to the Eval command's logic
        eval_cmd.execute(ctx).await
    }
}

impl CommandSpec for EvalSha {
    fn name(&self) -> &'static str {
        "evalsha"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE // Assume write by default
    }
    fn first_key(&self) -> i64 {
        3
    }
    fn last_key(&self) -> i64 {
        if self.num_keys > 0 {
            2 + self.num_keys as i64
        } else {
            0
        }
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.sha1.clone().into(), self.num_keys.to_string().into()];
        args.extend(self.keys.clone());
        args.extend(self.args.clone());
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

    #[test]
    fn test_parse_too_few_args_errors() {
        assert!(EvalSha::parse(&[]).is_err());
        assert!(EvalSha::parse(&[bulk("abc")]).is_err());
    }

    #[test]
    fn test_parse_no_keys() {
        let cmd = EvalSha::parse(&[bulk("sha1hash"), bulk("0")]).unwrap();
        assert_eq!(cmd.sha1, "sha1hash");
        assert_eq!(cmd.num_keys, 0);
        assert!(cmd.keys.is_empty());
        assert!(cmd.args.is_empty());
    }

    #[test]
    fn test_parse_with_keys_no_args() {
        let cmd = EvalSha::parse(&[bulk("sha1"), bulk("2"), bulk("k1"), bulk("k2")]).unwrap();
        assert_eq!(cmd.sha1, "sha1");
        assert_eq!(cmd.num_keys, 2);
        assert_eq!(
            cmd.keys,
            vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]
        );
        assert!(cmd.args.is_empty());
    }

    #[test]
    fn test_parse_with_keys_and_args() {
        let cmd = EvalSha::parse(&[
            bulk("sha1"),
            bulk("1"),
            bulk("mykey"),
            bulk("arg1"),
            bulk("arg2"),
        ])
        .unwrap();
        assert_eq!(cmd.num_keys, 1);
        assert_eq!(cmd.keys, vec![Bytes::from_static(b"mykey")]);
        assert_eq!(
            cmd.args,
            vec![Bytes::from_static(b"arg1"), Bytes::from_static(b"arg2")]
        );
    }

    #[test]
    fn test_parse_num_keys_exceeds_args_errors() {
        let result = EvalSha::parse(&[bulk("sha1"), bulk("3"), bulk("k1"), bulk("k2")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_bad_num_keys_errors() {
        let result = EvalSha::parse(&[bulk("sha1"), bulk("notanumber")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_negative_num_keys_errors() {
        let result = EvalSha::parse(&[bulk("sha1"), bulk("-1")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_command_name() {
        assert_eq!(EvalSha::default().name(), "evalsha");
    }

    #[test]
    fn test_command_arity() {
        assert_eq!(EvalSha::default().arity(), -3);
    }

    #[test]
    fn test_command_flags() {
        assert!(EvalSha::default().flags().contains(CommandFlags::WRITE));
    }

    #[test]
    fn test_get_keys_with_keys() {
        let cmd = EvalSha {
            num_keys: 2,
            keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
            ..Default::default()
        };
        assert_eq!(
            cmd.get_keys(),
            vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]
        );
    }

    #[test]
    fn test_get_keys_no_keys() {
        let cmd = EvalSha::default();
        assert!(cmd.get_keys().is_empty());
    }

    #[test]
    fn test_first_key() {
        assert_eq!(EvalSha::default().first_key(), 3);
    }

    #[test]
    fn test_last_key_with_keys() {
        let cmd = EvalSha {
            num_keys: 2,
            ..Default::default()
        };
        assert_eq!(cmd.last_key(), 4);
    }

    #[test]
    fn test_last_key_no_keys() {
        let cmd = EvalSha {
            num_keys: 0,
            ..Default::default()
        };
        assert_eq!(cmd.last_key(), 0);
    }

    #[test]
    fn test_step() {
        assert_eq!(EvalSha::default().step(), 1);
    }

    #[test]
    fn test_to_resp_args_no_keys_no_extra_args() {
        let cmd = EvalSha {
            sha1: "abc".into(),
            num_keys: 0,
            keys: vec![],
            args: vec![],
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"abc"));
        assert_eq!(args[1], Bytes::from_static(b"0"));
    }

    #[test]
    fn test_to_resp_args_full() {
        let cmd = EvalSha {
            sha1: "sha".into(),
            num_keys: 1,
            keys: vec![Bytes::from_static(b"k")],
            args: vec![Bytes::from_static(b"a")],
        };
        let args = cmd.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[0], Bytes::from_static(b"sha"));
        assert_eq!(args[1], Bytes::from_static(b"1"));
        assert_eq!(args[2], Bytes::from_static(b"k"));
        assert_eq!(args[3], Bytes::from_static(b"a"));
    }
}
