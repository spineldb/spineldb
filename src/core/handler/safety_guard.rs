// src/core/handler/safety_guard.rs

//! Implements a circuit breaker to prevent dangerous commands from being executed.

use crate::core::Command;
use crate::core::SpinelDBError;
use crate::core::commands::command_trait::CommandExt;
use crate::core::state::ServerState;
use crate::core::storage::data_types::DataValue;
use std::sync::Arc;

/// Checks if a command violates configured safety limits.
pub async fn check_safety_limits(
    state: &Arc<ServerState>,
    command: &Command,
    current_db_index: usize,
) -> Result<(), SpinelDBError> {
    let config_guard = state.config.lock().await;
    let safety_config = &config_guard.safety;

    // Check for commands that scan potentially large collections.
    if safety_config.max_collection_scan_keys > 0 {
        match command {
            Command::Keys(_)
            | Command::Sort(_)
            | Command::Smembers(_)
            | Command::HGetAll(_)
            | Command::HKeys(_)
            | Command::HVals(_) => {
                let key = &command.get_keys()[0];
                let db = state.get_db(current_db_index).unwrap();
                let shard_index = db.get_shard_index(key);
                let guard = db.get_shard(shard_index).entries.lock().await;

                if let Some(entry) = guard.peek(key)
                    && !entry.is_expired()
                {
                    let len = match &entry.data {
                        DataValue::List(l) => l.len(),
                        DataValue::Set(s) => s.len(),
                        DataValue::Hash(h) => h.len(),
                        DataValue::SortedSet(z) => z.len(),
                        DataValue::HyperLogLog(hll) => hll.count() as usize,
                        _ => 0,
                    };

                    if len > safety_config.max_collection_scan_keys {
                        return Err(SpinelDBError::InvalidState(format!(
                            "Command '{}' on key '{}' aborted: collection size ({}) exceeds 'max_collection_scan_keys' limit ({}). Use SCAN-family commands instead.",
                            command.name(),
                            String::from_utf8_lossy(key),
                            len,
                            safety_config.max_collection_scan_keys
                        )));
                    }
                }
            }
            _ => {}
        }
    }

    // Check for set operations with too many input keys.
    if safety_config.max_set_operation_keys > 0 {
        let (keys_len, is_set_op) = match command {
            Command::SUnion(c) => (c.keys.len(), true),
            Command::SInter(c) => (c.keys.len(), true),
            Command::Sdiff(c) => (c.keys.len(), true),
            Command::SUnionStore(c) => (c.keys.len(), true),
            Command::SInterStore(c) => (c.keys.len(), true),
            Command::SdiffStore(c) => (c.keys.len(), true),
            _ => (0, false),
        };

        if is_set_op && keys_len > safety_config.max_set_operation_keys {
            return Err(SpinelDBError::InvalidState(format!(
                "Command '{}' aborted: number of keys ({}) exceeds 'max_set_operation_keys' limit ({}).",
                command.name(),
                keys_len,
                safety_config.max_set_operation_keys
            )));
        }
    }

    // Check for BITOP allocation size.
    if safety_config.max_bitop_alloc_size > 0
        && let Command::BitOp(c) = command
    {
        let db = state.get_db(current_db_index).unwrap();
        let mut max_len = 0;
        for key in &c.src_keys {
            let shard_index = db.get_shard_index(key);
            let guard = db.get_shard(shard_index).entries.lock().await;
            if let Some(entry) = guard.peek(key)
                && let DataValue::String(s) = &entry.data
            {
                max_len = max_len.max(s.len());
            }
        }
        if max_len > safety_config.max_bitop_alloc_size {
            return Err(SpinelDBError::InvalidState(format!(
                "Command 'BITOP' aborted: required allocation ({}) exceeds 'max_bitop_alloc_size' limit ({}).",
                max_len, safety_config.max_bitop_alloc_size
            )));
        }
    }

    Ok(())
}
