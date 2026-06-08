// src/core/commands/list/pushx.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::parse_key_and_values;
use crate::core::database::{ExecutionContext, PushDirection};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;

// Helper logic untuk kedua perintah PUSHX
async fn pushx_logic<'a>(
    ctx: &mut ExecutionContext<'a>,
    key: &Bytes,
    values: &[Bytes],
    direction: PushDirection,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    if values.is_empty() {
        return Err(SpinelDBError::SyntaxError); // SpinelDB mengembalikan error jika tidak ada value
    }

    let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
    let Some(entry) = shard_cache_guard.get_mut(key) else {
        return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite)); // Kunci tidak ada
    };

    if entry.is_expired() {
        shard_cache_guard.pop(key);
        return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite)); // Kunci tidak ada (karena expired)
    }

    if let DataValue::List(list) = &mut entry.data {
        let mut total_added_size = 0;
        for value in values {
            total_added_size += value.len();
            match direction {
                PushDirection::Left => list.push_front(value.clone()),
                PushDirection::Right => list.push_back(value.clone()),
            }
        }
        entry.version = entry.version.wrapping_add(1);
        entry.size += total_added_size;
        shard
            .current_memory
            .fetch_add(total_added_size, Ordering::Relaxed);

        Ok((
            RespValue::Integer(list.len() as i64),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    } else {
        Err(SpinelDBError::WrongType)
    }
}

// --- LPUSHX ---
#[derive(Debug, Clone, Default)]
pub struct LPushX {
    pub key: Bytes,
    pub values: Vec<Bytes>,
}

impl ParseCommand for LPushX {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, values) = parse_key_and_values(args, 2, "LPUSHX")?;
        Ok(LPushX { key, values })
    }
}

#[async_trait]
impl ExecutableCommand for LPushX {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        pushx_logic(ctx, &self.key, &self.values, PushDirection::Left).await
    }
}

impl CommandSpec for LPushX {
    fn name(&self) -> &'static str {
        "lpushx"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone()];
        args.extend(self.values.clone());
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
    fn test_lpushx_parses_key_and_values() {
        let c = LPushX::parse(&[bs("k"), bs("v1"), bs("v2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.values.len(), 2);
    }

    #[test]
    fn test_lpushx_with_no_values_is_error() {
        // LPUSHX must have at least one value (pushx_logic returns SyntaxError if values empty).
        let r = LPushX::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_lpushx_with_non_bulk_key_is_wrong_type() {
        let r = LPushX::parse(&[RespFrame::Integer(1), bs("v")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_lpushx_to_resp_args_round_trips() {
        let c = LPushX::parse(&[bs("k"), bs("a"), bs("b")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], Bytes::from_static(b"k"));
    }
}

// --- RPUSHX ---
#[derive(Debug, Clone, Default)]
pub struct RPushX {
    pub key: Bytes,
    pub values: Vec<Bytes>,
}

impl ParseCommand for RPushX {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let (key, values) = parse_key_and_values(args, 2, "RPUSHX")?;
        Ok(RPushX { key, values })
    }
}

#[async_trait]
impl ExecutableCommand for RPushX {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        pushx_logic(ctx, &self.key, &self.values, PushDirection::Right).await
    }
}

impl CommandSpec for RPushX {
    fn name(&self) -> &'static str {
        "rpushx"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone()];
        args.extend(self.values.clone());
        args
    }
}

#[cfg(test)]
mod rpushx_tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_rpushx_parses_key_and_values() {
        let c = RPushX::parse(&[bs("k"), bs("v1"), bs("v2")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"k"));
        assert_eq!(c.values.len(), 2);
    }

    #[test]
    fn test_rpushx_with_no_values_is_error() {
        let r = RPushX::parse(&[bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_rpushx_with_non_bulk_key_is_wrong_type() {
        let r = RPushX::parse(&[RespFrame::Integer(1), bs("v")]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_rpushx_to_resp_args_round_trips() {
        let c = RPushX::parse(&[bs("k"), bs("a")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(
            args,
            vec![Bytes::from_static(b"k"), Bytes::from_static(b"a")]
        );
    }
}
