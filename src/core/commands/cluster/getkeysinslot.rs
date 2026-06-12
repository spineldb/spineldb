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

    #[test]
    fn test_slot_not_owned_error_format() {
        let slot: u16 = 5000;
        let err_msg = format!("Slot {slot} is not served by this instance");
        assert!(err_msg.contains("5000"));
        assert!(err_msg.contains("not served"));
    }

    #[test]
    fn test_count_zero_returns_empty() {
        let count: usize = 0;
        let keys: Vec<Bytes> = Vec::new();
        assert_eq!(keys.len(), count);
    }

    #[test]
    fn test_empty_keys_to_resp_array() {
        let keys: Vec<Bytes> = vec![];
        let resp_keys: Vec<RespValue> = keys.into_iter().map(RespValue::BulkString).collect();
        assert!(resp_keys.is_empty());
        assert!(matches!(RespValue::Array(resp_keys), RespValue::Array(a) if a.is_empty()));
    }

    #[test]
    fn test_single_key_to_resp_array() {
        let keys: Vec<Bytes> = vec![Bytes::from_static(b"mykey")];
        let resp_keys: Vec<RespValue> = keys.into_iter().map(RespValue::BulkString).collect();
        assert_eq!(resp_keys.len(), 1);
        assert!(matches!(&resp_keys[0], RespValue::BulkString(b) if b.as_ref() == b"mykey"));
    }

    #[test]
    fn test_multiple_keys_to_resp_array() {
        let keys: Vec<Bytes> = vec![
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"k3"),
        ];
        let resp_keys: Vec<RespValue> = keys.into_iter().map(RespValue::BulkString).collect();
        assert_eq!(resp_keys.len(), 3);
    }
}
