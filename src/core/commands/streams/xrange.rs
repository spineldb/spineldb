// src/core/commands/streams/xrange.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::stream::{StreamEntry, StreamId};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Bound;

#[derive(Debug, Clone, Default)]
pub struct XRange {
    pub key: Bytes,
    pub start: StreamId,
    pub end: StreamId,
    pub count: Option<usize>,
    pub is_rev: bool,
}

#[derive(Debug, Clone, Default)]
pub struct XRevRange(pub XRange);

fn parse_range_boundary(s: &str) -> Result<StreamId, SpinelDBError> {
    if s == "-" {
        Ok(StreamId::new(0, 0))
    } else if s == "+" {
        Ok(StreamId::new(u64::MAX, u64::MAX))
    } else {
        // [PERBAIKAN] Menggunakan .parse() karena StreamId sekarang mengimplementasikan FromStr.
        s.parse::<StreamId>()
            .map_err(|e| SpinelDBError::InvalidState(e.to_string()))
    }
}

fn parse_xrange_args(
    args: &[RespFrame],
    cmd_name: &str,
) -> Result<(Bytes, StreamId, StreamId, Option<usize>), SpinelDBError> {
    if args.len() < 3 {
        return Err(SpinelDBError::WrongArgumentCount(cmd_name.to_string()));
    }
    let key = extract_bytes(&args[0])?;
    let start = parse_range_boundary(&extract_string(&args[1])?)?;
    let end = parse_range_boundary(&extract_string(&args[2])?)?;

    let mut count = None;
    if args.len() > 3 {
        if extract_string(&args[3])?.eq_ignore_ascii_case("COUNT") {
            if args.len() != 5 {
                return Err(SpinelDBError::SyntaxError);
            }
            count = Some(extract_string(&args[4])?.parse()?);
        } else {
            return Err(SpinelDBError::SyntaxError);
        }
    }
    Ok((key, start, end, count))
}

impl ParseCommand for XRange {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, start, end, count) = parse_xrange_args(args, "XRANGE")?;
        Ok(XRange {
            key,
            start,
            end,
            count,
            is_rev: false,
        })
    }
}

impl ParseCommand for XRevRange {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, start, end, count) = parse_xrange_args(args, "XREVRANGE")?;
        Ok(XRevRange(XRange {
            key,
            start: end,
            end: start,
            count,
            is_rev: true,
        }))
    }
}

#[async_trait]
impl ExecutableCommand for XRange {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, guard) = ctx.get_single_shard_context_mut()?;

        if let Some(entry) = guard.peek(&self.key) {
            if entry.is_expired() {
                return Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite));
            }

            if let DataValue::Stream(stream) = &entry.data {
                let range = stream
                    .entries
                    .range((Bound::Included(self.start), Bound::Included(self.end)));

                let results: Vec<RespValue> = if self.is_rev {
                    range
                        .rev()
                        .take(self.count.unwrap_or(usize::MAX))
                        .map(Self::format_entry)
                        .collect()
                } else {
                    range
                        .take(self.count.unwrap_or(usize::MAX))
                        .map(Self::format_entry)
                        .collect()
                };

                return Ok((RespValue::Array(results), WriteOutcome::DidNotWrite));
            } else {
                return Err(SpinelDBError::WrongType);
            }
        }

        Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite))
    }
}

impl XRange {
    pub fn format_entry((id, entry): (&StreamId, &StreamEntry)) -> RespValue {
        let mut fields_array = Vec::with_capacity(entry.fields.len() * 2);
        for (k, v) in &entry.fields {
            fields_array.push(RespValue::BulkString(k.clone()));
            fields_array.push(RespValue::BulkString(v.clone()));
        }
        RespValue::Array(vec![
            RespValue::BulkString(id.to_string().into()),
            RespValue::Array(fields_array),
        ])
    }
}

#[async_trait]
impl ExecutableCommand for XRevRange {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        self.0.execute(ctx).await
    }
}

impl CommandSpec for XRange {
    fn name(&self) -> &'static str {
        "xrange"
    }
    fn arity(&self) -> i64 {
        -4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
            self.start.to_string().into(),
            self.end.to_string().into(),
        ];
        if let Some(c) = self.count {
            args.extend([Bytes::from_static(b"COUNT"), c.to_string().into()]);
        }
        args
    }
}

impl CommandSpec for XRevRange {
    fn name(&self) -> &'static str {
        "xrevrange"
    }
    fn arity(&self) -> i64 {
        -4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
        vec![self.0.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![
            self.0.key.clone(),
            self.0.end.to_string().into(),
            self.0.start.to_string().into(),
        ];
        if let Some(c) = self.0.count {
            args.extend([Bytes::from_static(b"COUNT"), c.to_string().into()]);
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
    fn test_xrange_parses_key_start_end() {
        let c = XRange::parse(&[bs("k"), bs("1-0"), bs("+")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert!(!c.is_rev);
        assert_eq!(c.count, None);
    }

    #[test]
    fn test_xrange_parses_minus_to_plus_range() {
        let c = XRange::parse(&[bs("k"), bs("-"), bs("+")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
    }

    #[test]
    fn test_xrange_parses_with_count() {
        let c = XRange::parse(&[bs("k"), bs("1-0"), bs("+"), bs("COUNT"), bs("5")]).unwrap();
        assert_eq!(c.count, Some(5));
    }

    #[test]
    fn test_xrange_count_case_insensitive() {
        let c = XRange::parse(&[bs("k"), bs("1-0"), bs("+"), bs("count"), bs("5")]).unwrap();
        assert_eq!(c.count, Some(5));
    }

    #[test]
    fn test_xrange_with_too_few_args_is_error() {
        let r = XRange::parse(&[bs("k"), bs("1-0")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_xrange_with_count_missing_value_is_syntax_error() {
        let r = XRange::parse(&[bs("k"), bs("1-0"), bs("+"), bs("COUNT")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_xrange_with_unknown_option_is_syntax_error() {
        let r = XRange::parse(&[bs("k"), bs("1-0"), bs("+"), bs("FOO")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_xrange_with_invalid_start_is_error() {
        let r = XRange::parse(&[bs("k"), bs("invalid"), bs("+")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidState(_))));
    }

    #[test]
    fn test_xrange_to_resp_args_round_trips() {
        let c = XRange::parse(&[bs("k"), bs("1-0"), bs("+"), bs("COUNT"), bs("3")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 5);
        assert_eq!(args[3], Bytes::from_static(b"COUNT"));
    }

    #[test]
    fn test_xrevrange_parses_key_end_start() {
        let c = XRevRange::parse(&[bs("k"), bs("+"), bs("1-0")]).unwrap();
        assert!(c.0.is_rev);
    }

    #[test]
    fn test_xrevrange_to_resp_args_swaps_start_end() {
        let c = XRevRange::parse(&[bs("k"), bs("+"), bs("1-0")]).unwrap();
        let args = c.to_resp_args();
        // Output should have end first, then start.
        let end_str: Bytes = c.0.end.to_string().into();
        let start_str: Bytes = c.0.start.to_string().into();
        assert_eq!(args[1], end_str);
        assert_eq!(args[2], start_str);
    }

    #[test]
    fn test_xrevrange_with_count() {
        let c = XRevRange::parse(&[bs("k"), bs("+"), bs("1-0"), bs("COUNT"), bs("5")]).unwrap();
        assert_eq!(c.0.count, Some(5));
    }
}
