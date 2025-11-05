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
