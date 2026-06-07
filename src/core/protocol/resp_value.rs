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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::protocol::RespFrame;

    #[test]
    fn test_simple_string_conversion() {
        let v = RespValue::SimpleString("OK".to_string());
        let f: RespFrame = v.into();
        assert_eq!(f, RespFrame::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_bulk_string_conversion() {
        let v = RespValue::BulkString(Bytes::from_static(b"hello"));
        let f: RespFrame = v.into();
        assert_eq!(f, RespFrame::BulkString(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_integer_conversion() {
        let v = RespValue::Integer(-42);
        let f: RespFrame = v.into();
        assert_eq!(f, RespFrame::Integer(-42));
    }

    #[test]
    fn test_null_variants_conversion() {
        assert_eq!(RespFrame::from(RespValue::Null), RespFrame::Null);
        assert_eq!(RespFrame::from(RespValue::NullArray), RespFrame::NullArray);
    }

    #[test]
    fn test_error_conversion() {
        let v = RespValue::Error("ERR oops".to_string());
        let f: RespFrame = v.into();
        assert_eq!(f, RespFrame::Error("ERR oops".to_string()));
    }

    #[test]
    fn test_array_conversion_is_recursive() {
        let v = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::BulkString(Bytes::from_static(b"two")),
            RespValue::Array(vec![RespValue::Null, RespValue::Integer(3)]),
        ]);
        let f: RespFrame = v.into();
        let expected = RespFrame::Array(vec![
            RespFrame::Integer(1),
            RespFrame::BulkString(Bytes::from_static(b"two")),
            RespFrame::Array(vec![RespFrame::Null, RespFrame::Integer(3)]),
        ]);
        assert_eq!(f, expected);
    }
}
