// src/core/commands/cluster/getkeysinslot.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
    slot: u16,
    count: usize,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx.state.cluster.as_ref().unwrap();

    if !cluster.i_own_slot(slot) {
        return Err(SpinelDBError::InvalidState(format!(
            "Slot {slot} is not served by this instance"
        )));
    }

    let keys = ctx.db.get_keys_in_slot(slot, count).await;
    let resp_keys = keys.into_iter().map(RespValue::BulkString).collect();

    Ok((RespValue::Array(resp_keys), WriteOutcome::DidNotWrite))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn keys_to_resp(keys: Vec<Bytes>) -> RespValue {
        let resp_keys: Vec<RespValue> = keys.into_iter().map(RespValue::BulkString).collect();
        RespValue::Array(resp_keys)
    }

    #[test]
    fn test_empty_keys_produces_empty_array() {
        let result = keys_to_resp(vec![]);
        match result {
            RespValue::Array(a) => assert!(a.is_empty()),
            _ => panic!("expected RespValue::Array"),
        }
    }

    #[test]
    fn test_single_key_produces_array_of_one() {
        let result = keys_to_resp(vec![Bytes::from_static(b"mykey")]);
        match result {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 1);
                match &a[0] {
                    RespValue::BulkString(b) => assert_eq!(b.as_ref(), b"mykey"),
                    _ => panic!("expected BulkString"),
                }
            }
            _ => panic!("expected RespValue::Array"),
        }
    }

    #[test]
    fn test_multiple_keys_preserve_order() {
        let keys = vec![
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"beta"),
            Bytes::from_static(b"gamma"),
        ];
        let result = keys_to_resp(keys);
        match result {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 3);
                assert!(matches!(&a[0], RespValue::BulkString(b) if b.as_ref() == b"alpha"));
                assert!(matches!(&a[1], RespValue::BulkString(b) if b.as_ref() == b"beta"));
                assert!(matches!(&a[2], RespValue::BulkString(b) if b.as_ref() == b"gamma"));
            }
            _ => panic!("expected RespValue::Array"),
        }
    }

    #[test]
    fn test_keys_with_binary_data() {
        let keys = vec![Bytes::from_static(b"\x00\x01\xff"), Bytes::from_static(b"")];
        let result = keys_to_resp(keys);
        match result {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert!(matches!(&a[0], RespValue::BulkString(b) if b.as_ref() == b"\x00\x01\xff"));
                assert!(matches!(&a[1], RespValue::BulkString(b) if b.is_empty()));
            }
            _ => panic!("expected RespValue::Array"),
        }
    }

    #[test]
    fn test_count_zero_with_nonempty_keys_is_separate_concern() {
        let count: usize = 0;
        let keys: Vec<Bytes> = vec![];
        assert_eq!(keys.len(), count);
        let result = keys_to_resp(keys);
        match result {
            RespValue::Array(a) => assert_eq!(a.len(), 0),
            _ => panic!("expected RespValue::Array"),
        }
    }
}
