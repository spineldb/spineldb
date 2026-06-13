// src/core/commands/generic/hello.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct Hello {
    pub proto: u8,
}

impl ParseCommand for Hello {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        let mut proto: u8 = 3; // Default to RESP3 for full protocol support
        if let Some(arg) = args.first() {
            match arg {
                RespFrame::BulkString(bs) => {
                    let s = String::from_utf8_lossy(bs);
                    proto = s.parse::<u8>().map_err(|_| SpinelDBError::SyntaxError)?;
                }
                _ => {
                    return Err(SpinelDBError::WrongType);
                }
            }
            if proto != 2 && proto != 3 {
                return Err(SpinelDBError::InvalidRequest(
                    "NOPROTO unsupported protocol version".to_string(),
                ));
            }
        }
        Ok(Self { proto })
    }
}

#[async_trait]
impl ExecutableCommand for Hello {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Sync protocol_version to ClientInfo so CLIENT LIST reflects the current state.
        if let Some(entry) = ctx.state.clients.get(&ctx.session_id) {
            let (client_info_arc, _) = entry.value();
            client_info_arc.lock().await.protocol_version = self.proto;
        }

        let map = vec![
            (
                RespValue::BulkString("server".into()),
                RespValue::BulkString("spineldb".into()),
            ),
            (
                RespValue::BulkString("version".into()),
                RespValue::BulkString(env!("CARGO_PKG_VERSION").into()),
            ),
            (
                RespValue::BulkString("proto".into()),
                RespValue::Integer(self.proto as i64),
            ),
            (
                RespValue::BulkString("id".into()),
                RespValue::Integer(ctx.session_id as i64),
            ),
            (
                RespValue::BulkString("mode".into()),
                RespValue::BulkString("standalone".into()),
            ),
            (
                RespValue::BulkString("role".into()),
                RespValue::BulkString("master".into()),
            ),
            (
                RespValue::BulkString("modules".into()),
                RespValue::Array(vec![]),
            ),
            (
                RespValue::BulkString("capa".into()),
                RespValue::Array(vec![
                    RespValue::BulkString("resp3".into()),
                    RespValue::BulkString("json".into()),
                    RespValue::BulkString("psync2".into()),
                ]),
            ),
        ];

        let result = if self.proto == 3 {
            RespValue::Map(map)
        } else {
            // RESP2 flat array fallback
            let mut flat = Vec::with_capacity(map.len() * 2);
            for (k, v) in map {
                flat.push(k);
                flat.push(v);
            }
            RespValue::Array(flat)
        };

        Ok((result, WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for Hello {
    fn name(&self) -> &'static str {
        "hello"
    }
    fn arity(&self) -> i64 {
        -1
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE
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
        vec![self.proto.to_string().into()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_hello_default_proto_is_3() {
        let h = Hello::parse(&[]).unwrap();
        assert_eq!(h.proto, 3);
    }

    #[test]
    fn test_hello_proto_2() {
        let h = Hello::parse(&[bs("2")]).unwrap();
        assert_eq!(h.proto, 2);
    }

    #[test]
    fn test_hello_proto_3() {
        let h = Hello::parse(&[bs("3")]).unwrap();
        assert_eq!(h.proto, 3);
    }

    #[test]
    fn test_hello_unsupported_proto_is_error() {
        let r = Hello::parse(&[bs("4")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_hello_with_non_bulk_is_wrong_type() {
        let r = Hello::parse(&[RespFrame::Integer(3)]);
        assert!(matches!(r, Err(SpinelDBError::WrongType)));
    }

    #[test]
    fn test_hello_to_resp_args() {
        let h = Hello::parse(&[bs("3")]).unwrap();
        assert_eq!(h.to_resp_args(), vec![Bytes::from_static(b"3")]);
    }
}
