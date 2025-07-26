// src/core/commands/string/append.rs

use crate::config::EvictionPolicy;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, MAX_STRING_SIZE, StoredValue};
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, Default)]
pub struct Append {
    pub key: Bytes,
    pub value: Bytes,
}

impl ParseCommand for Append {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 2, "APPEND")?;
        Ok(Append {
            key: extract_bytes(&args[0])?,
            value: extract_bytes(&args[1])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Append {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (maxmemory, policy) = {
            let config = ctx.state.config.lock().await;
            (config.maxmemory, config.maxmemory_policy)
        };

        if let Some(maxmem) = maxmemory {
            const MAX_EVICTION_ATTEMPTS: usize = 10;
            for _ in 0..MAX_EVICTION_ATTEMPTS {
                let total_memory: usize =
                    ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();
                let estimated_increase = self.value.len();

                if total_memory.saturating_add(estimated_increase) <= maxmem {
                    break;
                }

                if policy == EvictionPolicy::NoEviction {
                    return Err(SpinelDBError::MaxMemoryReached);
                }

                if !ctx.db.evict_one_key(&ctx.state).await {
                    break;
                }
            }

            let total_memory: usize = ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();
            if total_memory.saturating_add(self.value.len()) > maxmem {
                return Err(SpinelDBError::MaxMemoryReached);
            }
        }

        let final_len;

        // --- Start of Locking Scope ---
        {
            let (shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

            let entry = shard_cache_guard.get_or_insert_with_mut(self.key.clone(), || {
                StoredValue::new(DataValue::String(Bytes::new()))
            });

            if entry.is_expired() {
                entry.data = DataValue::String(Bytes::new());
                entry.expiry = None;
            }

            if let DataValue::String(s) = &mut entry.data {
                let required_len = s.len().saturating_add(self.value.len());
                if required_len > MAX_STRING_SIZE {
                    return Err(SpinelDBError::InvalidState(
                        "string length is greater than maximum allowed size (512MB)".to_string(),
                    ));
                }

                let mut new_bytes = BytesMut::with_capacity(required_len);
                new_bytes.extend_from_slice(s);
                new_bytes.extend_from_slice(&self.value);
                *s = new_bytes.freeze();
            } else {
                return Err(SpinelDBError::WrongType);
            };

            final_len = entry.data.memory_usage();
            let old_size = entry.size;
            let mem_diff = final_len as isize - old_size as isize;

            entry.size = final_len;
            entry.version = entry.version.wrapping_add(1);

            shard.update_memory(mem_diff);
        } // --- End of Locking Scope ---

        Ok((
            RespValue::Integer(final_len as i64),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }
}

impl CommandSpec for Append {
    fn name(&self) -> &'static str {
        "append"
    }
    fn arity(&self) -> i64 {
        3
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
        vec![self.key.clone(), self.value.clone()]
    }
}
