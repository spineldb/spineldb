// src/core/protocol/resp_value.rs

//! Defines a simplified value type for use within the command execution layer.

use bytes::Bytes;

/// `RespValue` is a simplified version of `RespFrame`.
///
/// It's used as the return type for command execution logic. This abstraction is useful
/// because the command layer shouldn't need to worry about the full complexity of the
/// RESP protocol (e.g., it only needs to produce values, not necessarily parse them).
///
/// It can be easily converted into a `RespFrame` before being sent over the network.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    BulkString(Bytes),
    Integer(i64),
    Array(Vec<RespValue>),
    Null,
    NullArray,
    Error(String),
}

/// Implements the conversion from the internal `RespValue` to the wire-protocol `RespFrame`.
impl From<RespValue> for super::RespFrame {
    fn from(val: RespValue) -> Self {
        match val {
            RespValue::SimpleString(s) => super::RespFrame::SimpleString(s),
            RespValue::BulkString(b) => super::RespFrame::BulkString(b),
            RespValue::Integer(i) => super::RespFrame::Integer(i),
            // Recursively convert elements of an array.
            RespValue::Array(arr) => {
                super::RespFrame::Array(arr.into_iter().map(Into::into).collect())
            }
            RespValue::Null => super::RespFrame::Null,
            RespValue::NullArray => super::RespFrame::NullArray,
            RespValue::Error(s) => super::RespFrame::Error(s),
        }
    }
}
