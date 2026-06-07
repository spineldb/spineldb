// src/core/commands/list/blmove.rs

//! Implements the `BLMOVE` command, a blocking version of `LMOVE`.

use super::lmove::Side;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

/// The parsed `BLMOVE` command with its arguments.
#[derive(Debug, Clone, Default)]
pub struct BLMove {
    pub source: Bytes,
    pub destination: Bytes,
    pub from: Side,
    pub to: Side,
    pub timeout: Duration,
}

impl ParseCommand for BLMove {
    /// Parses the `BLMOVE` command arguments from the RESP frame.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 5, "BLMOVE")?;
        let source = extract_bytes(&args[0])?;
        let destination = extract_bytes(&args[1])?;
        let from = match extract_string(&args[2])?.to_ascii_lowercase().as_str() {
            "left" => Side::Left,
            "right" => Side::Right,
            _ => return Err(SpinelDBError::SyntaxError),
        };
        let to = match extract_string(&args[3])?.to_ascii_lowercase().as_str() {
            "left" => Side::Left,
            "right" => Side::Right,
            _ => return Err(SpinelDBError::SyntaxError),
        };

        let timeout_secs: f64 = extract_string(&args[4])?
            .parse()
            .map_err(|_| SpinelDBError::NotAFloat)?;

        // A timeout of 0 means block indefinitely.
        let timeout_duration = if timeout_secs <= 0.0 {
            Duration::from_secs(u64::MAX) // Effectively infinite
        } else {
            Duration::from_secs_f64(timeout_secs)
        };

        Ok(BLMove {
            source,
            destination,
            from,
            to,
            timeout: timeout_duration,
        })
    }
}

#[async_trait]
impl ExecutableCommand for BLMove {
    /// Executes the `BLMOVE` command.
    /// The complex blocking logic, including race condition prevention, is
    /// delegated to the central `BlockerManager`.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let state = ctx.state.clone();
        state
            .blocker_manager
            .orchestrate_blmove(
                ctx,
                &self.source,
                &self.destination,
                self.from,
                self.to,
                self.timeout,
            )
            .await
    }
}

impl CommandSpec for BLMove {
    fn name(&self) -> &'static str {
        "blmove"
    }
    fn arity(&self) -> i64 {
        6
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
            | CommandFlags::DENY_OOM
            | CommandFlags::NO_PROPAGATE
            | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        2
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.source.clone(), self.destination.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![
            self.source.clone(),
            self.destination.clone(),
            (if self.from == Side::Left {
                "LEFT"
            } else {
                "RIGHT"
            })
            .into(),
            (if self.to == Side::Left {
                "LEFT"
            } else {
                "RIGHT"
            })
            .into(),
            self.timeout.as_secs_f64().to_string().into(),
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
    fn test_blmove_parses_args() {
        let c = BLMove::parse(&[bs("src"), bs("dst"), bs("LEFT"), bs("RIGHT"), bs("5")]).unwrap();
        assert_eq!(c.source, Bytes::from_static(b"src"));
        assert_eq!(c.destination, Bytes::from_static(b"dst"));
        assert_eq!(c.from, Side::Left);
        assert_eq!(c.to, Side::Right);
        assert_eq!(c.timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_blmove_zero_timeout_becomes_max() {
        let c = BLMove::parse(&[bs("src"), bs("dst"), bs("LEFT"), bs("RIGHT"), bs("0")]).unwrap();
        assert_eq!(c.timeout, Duration::from_secs(u64::MAX));
    }

    #[test]
    fn test_blmove_case_insensitive_sides() {
        let c = BLMove::parse(&[bs("src"), bs("dst"), bs("left"), bs("right"), bs("1")]).unwrap();
        assert_eq!(c.from, Side::Left);
        assert_eq!(c.to, Side::Right);
    }

    #[test]
    fn test_blmove_with_too_few_args_is_error() {
        let r = BLMove::parse(&[bs("src"), bs("dst"), bs("LEFT"), bs("RIGHT")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_blmove_with_too_many_args_is_error() {
        let r = BLMove::parse(&[
            bs("src"),
            bs("dst"),
            bs("LEFT"),
            bs("RIGHT"),
            bs("5"),
            bs("extra"),
        ]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_blmove_with_invalid_from_is_syntax_error() {
        let r = BLMove::parse(&[bs("src"), bs("dst"), bs("TOP"), bs("LEFT"), bs("5")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_blmove_with_invalid_to_is_syntax_error() {
        let r = BLMove::parse(&[bs("src"), bs("dst"), bs("LEFT"), bs("BOTTOM"), bs("5")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_blmove_with_non_float_timeout_is_error() {
        let r = BLMove::parse(&[bs("src"), bs("dst"), bs("LEFT"), bs("RIGHT"), bs("forever")]);
        assert!(matches!(r, Err(SpinelDBError::NotAFloat)));
    }

    #[test]
    fn test_blmove_to_resp_args_round_trips() {
        let c = BLMove::parse(&[bs("src"), bs("dst"), bs("LEFT"), bs("RIGHT"), bs("2")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 5);
        assert_eq!(args[0], Bytes::from_static(b"src"));
        assert_eq!(args[1], Bytes::from_static(b"dst"));
        assert_eq!(args[2], Bytes::from_static(b"LEFT"));
        assert_eq!(args[3], Bytes::from_static(b"RIGHT"));
    }
}
