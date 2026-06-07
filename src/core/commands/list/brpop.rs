// src/core/commands/list/brpop.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::{ExecutionContext, PopDirection};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct BRPop {
    pub keys: Vec<Bytes>,
    pub timeout: Duration,
}

impl ParseCommand for BRPop {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("BRPOP".to_string()));
        }
        let timeout_str = match args.last().unwrap() {
            RespFrame::BulkString(bs) => String::from_utf8_lossy(bs),
            _ => return Err(SpinelDBError::NotAnInteger),
        };
        let timeout_secs: f64 = timeout_str
            .parse()
            .map_err(|_| SpinelDBError::NotAnInteger)?;

        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        let timeout_duration = if timeout_secs <= 0.0 {
            Duration::from_secs(u64::MAX)
        } else {
            Duration::from_secs_f64(timeout_secs)
        };

        Ok(BRPop {
            keys,
            timeout: timeout_duration,
        })
    }
}

#[async_trait]
impl ExecutableCommand for BRPop {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state = ctx.state.clone();
        state
            .blocker_manager
            .orchestrate_blocking_pop(ctx, &self.keys, PopDirection::Right, self.timeout)
            .await
    }
}

impl CommandSpec for BRPop {
    fn name(&self) -> &'static str {
        "brpop"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::NO_PROPAGATE | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        -2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = self.keys.clone();
        args.push(self.timeout.as_secs_f64().to_string().into());
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
    fn test_brpop_parses_single_key_with_timeout() {
        let c = BRPop::parse(&[bs("k1"), bs("5")]).unwrap();
        assert_eq!(c.keys.len(), 1);
        assert_eq!(c.timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_brpop_parses_multiple_keys() {
        let c = BRPop::parse(&[bs("k1"), bs("k2"), bs("3")]).unwrap();
        assert_eq!(c.keys.len(), 2);
    }

    #[test]
    fn test_brpop_zero_timeout_becomes_max() {
        let c = BRPop::parse(&[bs("k1"), bs("0")]).unwrap();
        assert_eq!(c.timeout, Duration::from_secs(u64::MAX));
    }

    #[test]
    fn test_brpop_with_too_few_args_is_error() {
        let r = BRPop::parse(&[bs("k1")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_brpop_with_no_args_is_error() {
        let r = BRPop::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_brpop_with_non_integer_timeout_is_error() {
        let r = BRPop::parse(&[bs("k1"), bs("forever")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_brpop_to_resp_args_round_trips() {
        let c = BRPop::parse(&[bs("k1"), bs("1")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], Bytes::from_static(b"k1"));
    }
}
