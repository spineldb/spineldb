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
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),
    VerbatimString(String, Bytes),
    Attribute(Vec<(RespValue, RespValue)>, Box<RespValue>),
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
            RespValue::Boolean(b) => super::RespFrame::Boolean(b),
            RespValue::Double(d) => super::RespFrame::Double(d),
            RespValue::BigNumber(s) => super::RespFrame::BigNumber(s),
            RespValue::Map(m) => {
                super::RespFrame::Map(m.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
            RespValue::Set(s) => super::RespFrame::Set(s.into_iter().map(Into::into).collect()),
            RespValue::Push(p) => super::RespFrame::Push(p.into_iter().map(Into::into).collect()),
            RespValue::VerbatimString(fmt, data) => super::RespFrame::VerbatimString(fmt, data),
            RespValue::Attribute(attr, data) => super::RespFrame::Attribute(
                attr.into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
                Box::new((*data).into()),
            ),
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
    #[test]
    fn test_resp3_types_conversion() {
        let v = RespValue::Boolean(true);
        assert_eq!(RespFrame::from(v), RespFrame::Boolean(true));

        let v = RespValue::Double(123.456);
        assert_eq!(RespFrame::from(v), RespFrame::Double(123.456));

        let v = RespValue::BigNumber("1234567890".to_string());
        assert_eq!(
            RespFrame::from(v),
            RespFrame::BigNumber("1234567890".to_string())
        );

        let v = RespValue::Map(vec![(
            RespValue::SimpleString("key".to_string()),
            RespValue::Integer(1),
        )]);
        assert_eq!(
            RespFrame::from(v),
            RespFrame::Map(vec![(
                RespFrame::SimpleString("key".to_string()),
                RespFrame::Integer(1)
            )])
        );

        let v = RespValue::Set(vec![RespValue::Integer(1)]);
        assert_eq!(
            RespFrame::from(v),
            RespFrame::Set(vec![RespFrame::Integer(1)])
        );

        let v = RespValue::Push(vec![RespValue::SimpleString("message".to_string())]);
        assert_eq!(
            RespFrame::from(v),
            RespFrame::Push(vec![RespFrame::SimpleString("message".to_string())])
        );

        let v = RespValue::VerbatimString("txt".to_string(), Bytes::from_static(b"hello"));
        assert_eq!(
            RespFrame::from(v),
            RespFrame::VerbatimString("txt".to_string(), Bytes::from_static(b"hello"))
        );

        let v = RespValue::Attribute(
            vec![(
                RespValue::SimpleString("ttl".to_string()),
                RespValue::Integer(3600),
            )],
            Box::new(RespValue::Integer(42)),
        );
        assert_eq!(
            RespFrame::from(v),
            RespFrame::Attribute(
                vec![(
                    RespFrame::SimpleString("ttl".to_string()),
                    RespFrame::Integer(3600)
                )],
                Box::new(RespFrame::Integer(42)),
            )
        );
    }
}
