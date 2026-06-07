// src/core/commands/zset/bzpopmax.rs

use super::zpop_logic::PopSide;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_bytes;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

/// Represents the `BZPOPMAX` command with its parsed arguments.
#[derive(Debug, Clone, Default)]
pub struct BZPopMax {
    pub keys: Vec<Bytes>,
    pub timeout: Duration,
}

impl ParseCommand for BZPopMax {
    /// Parses the `BZPOPMAX` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("BZPOPMAX".to_string()));
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

        let timeout = if timeout_secs <= 0.0 {
            Duration::from_secs(u64::MAX)
        } else {
            Duration::from_secs_f64(timeout_secs)
        };

        Ok(BZPopMax { keys, timeout })
    }
}

#[async_trait]
impl ExecutableCommand for BZPopMax {
    /// Executes the `BZPOPMAX` command.
    /// The complex blocking logic is delegated to the central `BlockerManager`.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state = ctx.state.clone();
        state
            .blocker_manager
            .orchestrate_zset_blocking_pop(ctx, &self.keys, PopSide::Max, self.timeout)
            .await
    }
}

impl CommandSpec for BZPopMax {
    fn name(&self) -> &'static str {
        "bzpopmax"
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
    fn test_bzpopmax_parses_single_key_with_timeout() {
        let c = BZPopMax::parse(&[bs("z1"), bs("5")]).unwrap();
        assert_eq!(c.keys.len(), 1);
        assert_eq!(c.timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_bzpopmax_parses_multiple_keys() {
        let c = BZPopMax::parse(&[bs("z1"), bs("z2"), bs("3")]).unwrap();
        assert_eq!(c.keys.len(), 2);
    }

    #[test]
    fn test_bzpopmax_zero_timeout_becomes_max() {
        let c = BZPopMax::parse(&[bs("z1"), bs("0")]).unwrap();
        assert_eq!(c.timeout, Duration::from_secs(u64::MAX));
    }

    #[test]
    fn test_bzpopmax_with_too_few_args_is_error() {
        let r = BZPopMax::parse(&[bs("z1")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_bzpopmax_with_non_integer_timeout_is_error() {
        let r = BZPopMax::parse(&[bs("z1"), bs("forever")]);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_bzpopmax_to_resp_args_round_trips() {
        let c = BZPopMax::parse(&[bs("z1"), bs("1")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 2);
    }
}
