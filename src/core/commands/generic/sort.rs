// src/core/commands/generic/sort.rs

use crate::core::cluster::slot::get_slot as get_cluster_slot;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::{Db, ExecutionContext};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use ordered_float::NotNan;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use tokio::sync::MutexGuard;
use tracing::warn;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SortableWeight {
    Numeric(NotNan<f64>),
    Alpha(Bytes),
}

#[derive(Debug, Clone, Default)]
pub struct Sort {
    key: Bytes,
    by_pattern: Option<Bytes>,
    limit: Option<(usize, usize)>,
    get_patterns: Vec<Bytes>,
    order: SortOrder,
    alpha: bool,
    store_destination: Option<Bytes>,
}

impl ParseCommand for Sort {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("SORT".to_string()));
        }
        let mut cmd = Sort {
            key: extract_bytes(&args[0])?,
            ..Default::default()
        };
        let mut i = 1;
        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "by" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    cmd.by_pattern = Some(extract_bytes(&args[i])?);
                }
                "limit" => {
                    i += 1;
                    if i + 1 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let offset = extract_string(&args[i])?
                        .parse()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    i += 1;
                    let count = extract_string(&args[i])?
                        .parse()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    cmd.limit = Some((offset, count));
                }
                "get" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    cmd.get_patterns.push(extract_bytes(&args[i])?);
                }
                "asc" => cmd.order = SortOrder::Asc,
                "desc" => cmd.order = SortOrder::Desc,
                "alpha" => cmd.alpha = true,
                "store" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    cmd.store_destination = Some(extract_bytes(&args[i])?);
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
            i += 1;
        }
        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for Sort {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // --- Phase 1: Pre-flight data collection and version snapshotting ---
        let (source_elements, initial_versions, copied_values) = {
            let mut all_keys_to_lock = BTreeSet::new();
            all_keys_to_lock.insert(self.key.clone());
            if let Some(dest) = &self.store_destination {
                all_keys_to_lock.insert(dest.clone());
            }

            // Temporarily acquire all necessary locks to create a consistent snapshot.
            let mut guards = ctx
                .db
                .lock_shards_for_keys(&all_keys_to_lock.iter().cloned().collect::<Vec<_>>())
                .await;

            let Some(entry) = self.get_value_from_guards(&self.key, &mut guards, ctx.db)? else {
                return self.handle_empty_source(ctx).await;
            };

            let source_elements = self.get_source_elements_from_entry(&entry)?;

            self.collect_extra_keys(&source_elements, &mut all_keys_to_lock)?;

            // Cluster slot checks now that all keys are known.
            self.check_cluster_slots(&all_keys_to_lock.iter().cloned().collect::<Vec<_>>(), ctx)?;

            self.check_memory_usage_for_copy(&all_keys_to_lock, ctx)
                .await?;

            let mut versions = BTreeMap::new();
            let mut copied_values = BTreeMap::new();

            for key in &all_keys_to_lock {
                if let Some(value) = self.get_value_from_guards(key, &mut guards, ctx.db)? {
                    versions.insert(key.clone(), value.version);
                    copied_values.insert(key.clone(), value.data.clone());
                } else {
                    versions.insert(key.clone(), 0);
                }
            }
            (source_elements, versions, copied_values)
        }; // All locks are released here.

        if source_elements.is_empty() {
            return self.handle_empty_source(ctx).await;
        }

        // --- Phase 2: Perform slow operations (sorting) outside of the lock ---
        let weights = self.get_sortable_weights(&source_elements, &copied_values)?;
        let mut sortable_items: Vec<(SortableWeight, Bytes)> =
            weights.into_iter().zip(source_elements).collect();

        sortable_items.sort_unstable();
        if self.order == SortOrder::Desc {
            sortable_items.reverse();
        }

        let final_items: Vec<Bytes> = self
            .apply_limit(sortable_items)
            .into_iter()
            .map(|(_, item)| item)
            .collect();

        // --- Phase 3: Re-acquire locks, verify versions, and finalize operation ---
        {
            let mut guards = ctx
                .db
                .lock_shards_for_keys(&initial_versions.keys().cloned().collect::<Vec<_>>())
                .await;

            // Optimistic lock verification.
            for (key, original_version) in &initial_versions {
                let current_version = self
                    .get_value_from_guards(key, &mut guards, ctx.db)?
                    .map_or(0, |v| v.version);

                if current_version != *original_version {
                    warn!(
                        "SORT for key '{}' aborted due to concurrent modification of key '{}'.",
                        String::from_utf8_lossy(&self.key),
                        String::from_utf8_lossy(key)
                    );
                    // Return an empty result as per Redis behavior on WATCH failure.
                    return Ok((
                        if self.store_destination.is_some() {
                            RespValue::Integer(0)
                        } else {
                            RespValue::Array(vec![])
                        },
                        WriteOutcome::DidNotWrite,
                    ));
                }
            }

            // If verification passes, perform the final action (STORE or GET).
            if let Some(dest_key) = &self.store_destination {
                self.execute_store(dest_key, final_items, &mut guards, ctx.db)
            } else {
                self.execute_get(&final_items, &copied_values)
            }
        }
    }
}

impl Sort {
    /// Estimates the memory required to copy all values for the sort operation and
    /// checks if it exceeds the `maxmemory` limit.
    async fn check_memory_usage_for_copy(
        &self,
        all_keys: &BTreeSet<Bytes>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<(), SpinelDBError> {
        if let Some(maxmemory) = ctx.state.config.lock().await.maxmemory {
            let mut estimated_copy_size = 0;
            for key in all_keys {
                let shard_index = ctx.db.get_shard_index(key);
                // We assume locks are already held from the caller.
                let guard = ctx.db.get_shard(shard_index).entries.lock().await;
                if let Some(value) = guard.peek(key).filter(|e| !e.is_expired()) {
                    estimated_copy_size += value.size;
                }
            }

            // Add type annotation here to resolve the ambiguity.
            let total_memory: usize = ctx.state.dbs.iter().map(|db| db.get_current_memory()).sum();

            if total_memory.saturating_add(estimated_copy_size) > maxmemory {
                return Err(SpinelDBError::MaxMemoryReached);
            }
        }
        Ok(())
    }

    fn collect_extra_keys(
        &self,
        source_elements: &[Bytes],
        required_keys: &mut BTreeSet<Bytes>,
    ) -> Result<(), SpinelDBError> {
        if let Some(by_pattern) = &self.by_pattern
            && by_pattern.as_ref() != b"nosort"
        {
            for element in source_elements {
                let (key, _) = self.resolve_pattern(by_pattern, element);
                required_keys.insert(key);
            }
        }
        for get_pattern in &self.get_patterns {
            if get_pattern.as_ref() != b"#" {
                for element in source_elements {
                    let (key, _) = self.resolve_pattern(get_pattern, element);
                    required_keys.insert(key);
                }
            }
        }
        Ok(())
    }

    fn check_cluster_slots(
        &self,
        all_keys: &[Bytes],
        ctx: &ExecutionContext<'_>,
    ) -> Result<(), SpinelDBError> {
        let Some(cluster_state) = &ctx.state.cluster else {
            return Ok(());
        };
        if all_keys.is_empty() {
            return Ok(());
        }
        let first_slot = get_cluster_slot(&all_keys[0]);
        for key_for_check in all_keys {
            if get_cluster_slot(key_for_check) != first_slot {
                return Err(SpinelDBError::CrossSlot);
            }
        }
        if !cluster_state.i_own_slot(first_slot) {
            let owner_node = cluster_state.get_node_for_slot(first_slot);
            let addr = owner_node.map_or_else(String::new, |node| node.node_info.addr.clone());
            return Err(SpinelDBError::Moved {
                slot: first_slot,
                addr,
            });
        }
        Ok(())
    }

    fn get_value_from_guards<'b>(
        &self,
        key: &Bytes,
        guards: &mut BTreeMap<usize, MutexGuard<'b, crate::core::database::ShardCache>>,
        db: &Db,
    ) -> Result<Option<StoredValue>, SpinelDBError> {
        let shard_index = db.get_shard_index(key);
        let guard = guards.get_mut(&shard_index).ok_or(SpinelDBError::Internal(
            "Required shard lock missing".into(),
        ))?;
        Ok(guard.peek(key).filter(|e| !e.is_expired()).cloned())
    }

    fn get_source_elements_from_entry(
        &self,
        entry: &StoredValue,
    ) -> Result<Vec<Bytes>, SpinelDBError> {
        match &entry.data {
            DataValue::List(l) => Ok(l.iter().cloned().collect()),
            DataValue::Set(s) => Ok(s.iter().cloned().collect()),
            DataValue::SortedSet(z) => Ok(z.iter().map(|e| e.member.clone()).collect()),
            _ => Err(SpinelDBError::WrongType),
        }
    }

    async fn handle_empty_source<'b>(
        &self,
        ctx: &mut ExecutionContext<'b>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if let Some(dest_key) = &self.store_destination {
            let (_, guard) = ctx.get_single_shard_context_mut()?;
            let outcome = if guard.pop(dest_key).is_some() {
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::DidNotWrite
            };
            return Ok((RespValue::Integer(0), outcome));
        }
        Ok((RespValue::Array(vec![]), WriteOutcome::DidNotWrite))
    }

    fn get_sortable_weights(
        &self,
        elements: &[Bytes],
        copied_values: &BTreeMap<Bytes, DataValue>,
    ) -> Result<Vec<SortableWeight>, SpinelDBError> {
        let mut weights = Vec::with_capacity(elements.len());
        let use_external_weights =
            self.by_pattern.is_some() && self.by_pattern.as_deref() != Some(b"nosort");

        for element in elements {
            let weight_source_bytes = if use_external_weights {
                let (by_key, by_field) =
                    self.resolve_pattern(self.by_pattern.as_ref().unwrap(), element);
                self.fetch_value_from_copied(&by_key, by_field.as_ref(), copied_values)?
                    .unwrap_or_default()
            } else {
                element.clone()
            };
            let weight = if self.alpha {
                SortableWeight::Alpha(weight_source_bytes)
            } else {
                let s = String::from_utf8_lossy(&weight_source_bytes);
                let num = s.parse::<f64>().unwrap_or(0.0);
                SortableWeight::Numeric(NotNan::new(num).map_err(|_| SpinelDBError::NotAFloat)?)
            };
            weights.push(weight);
        }
        Ok(weights)
    }

    fn fetch_value_from_copied(
        &self,
        key: &Bytes,
        field: Option<&Bytes>,
        copied_values: &BTreeMap<Bytes, DataValue>,
    ) -> Result<Option<Bytes>, SpinelDBError> {
        match copied_values.get(key) {
            None => Ok(None),
            Some(data_value) => match (field, data_value) {
                (Some(f), DataValue::Hash(h)) => Ok(h.get(f).cloned()),
                (None, DataValue::String(s)) => Ok(Some(s.clone())),
                _ => {
                    warn!(
                        "SORT...BY/GET pattern points to key '{}' with incompatible type.",
                        String::from_utf8_lossy(key)
                    );
                    Err(SpinelDBError::WrongType)
                }
            },
        }
    }

    fn apply_limit(&self, items: Vec<(SortableWeight, Bytes)>) -> Vec<(SortableWeight, Bytes)> {
        if let Some((offset, count)) = self.limit {
            items.into_iter().skip(offset).take(count).collect()
        } else {
            items
        }
    }

    fn execute_store<'b>(
        &self,
        dest_key: &Bytes,
        items: Vec<Bytes>,
        guards: &mut BTreeMap<usize, MutexGuard<'b, crate::core::database::ShardCache>>,
        db: &Db,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let list: VecDeque<Bytes> = items.into_iter().collect();
        let list_len = list.len();
        let shard_index = db.get_shard_index(dest_key);
        let guard = guards
            .get_mut(&shard_index)
            .ok_or_else(|| SpinelDBError::Internal("Missing destination shard lock".into()))?;

        let new_value = StoredValue::new(DataValue::List(list));
        guard.put(dest_key.clone(), new_value);
        Ok((
            RespValue::Integer(list_len as i64),
            WriteOutcome::Write { keys_modified: 1 },
        ))
    }

    fn execute_get(
        &self,
        items: &[Bytes],
        copied_values: &BTreeMap<Bytes, DataValue>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        if self.get_patterns.is_empty() {
            let result = items
                .iter()
                .map(|item| RespValue::BulkString(item.clone()))
                .collect();
            return Ok((RespValue::Array(result), WriteOutcome::DidNotWrite));
        }
        let mut final_result = Vec::new();
        for item in items {
            for pattern in &self.get_patterns {
                let value = if pattern.as_ref() == b"#" {
                    Some(item.clone())
                } else {
                    let (get_key, get_field) = self.resolve_pattern(pattern, item);
                    self.fetch_value_from_copied(&get_key, get_field.as_ref(), copied_values)?
                };
                final_result.push(value.map(RespValue::BulkString).unwrap_or(RespValue::Null));
            }
        }
        Ok((RespValue::Array(final_result), WriteOutcome::DidNotWrite))
    }

    fn resolve_pattern(&self, pattern: &Bytes, element: &Bytes) -> (Bytes, Option<Bytes>) {
        let pattern_str = String::from_utf8_lossy(pattern);
        let element_str = String::from_utf8_lossy(element);

        match pattern_str.split_once("->") {
            Some((key_pattern, field)) => {
                let key = Bytes::from(key_pattern.replace('*', &element_str));
                let field = Bytes::from(field.to_string());
                (key, Some(field))
            }
            None => {
                let key = Bytes::from(pattern_str.replace('*', &element_str));
                (key, None)
            }
        }
    }
}

impl CommandSpec for Sort {
    fn name(&self) -> &'static str {
        "sort"
    }

    fn arity(&self) -> i64 {
        -2
    }

    fn flags(&self) -> CommandFlags {
        let mut flags = CommandFlags::MOVABLEKEYS;
        if self.store_destination.is_some() {
            flags.insert(CommandFlags::WRITE | CommandFlags::DENY_OOM);
        } else {
            flags.insert(CommandFlags::READONLY);
        }
        flags
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
        let mut keys = vec![self.key.clone()];
        if let Some(dest) = &self.store_destination {
            keys.push(dest.clone());
        }
        // Note: Dynamic keys from BY/GET are not included here as they are resolved at runtime.
        keys
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.key.clone()];
        if let Some(p) = &self.by_pattern {
            args.extend_from_slice(&[Bytes::from_static(b"BY"), p.clone()]);
        }
        if let Some((offset, count)) = &self.limit {
            args.extend_from_slice(&[
                Bytes::from_static(b"LIMIT"),
                offset.to_string().into(),
                count.to_string().into(),
            ]);
        }
        for p in &self.get_patterns {
            args.extend_from_slice(&[Bytes::from_static(b"GET"), p.clone()]);
        }
        if self.order == SortOrder::Desc {
            args.push(Bytes::from_static(b"DESC"));
        } else if !self.get_patterns.is_empty()
            || self.limit.is_some()
            || self.by_pattern.is_some()
            || self.alpha
        {
            // ASC is the default, but explicitly added if other options are present.
            args.push(Bytes::from_static(b"ASC"));
        }
        if self.alpha {
            args.push(Bytes::from_static(b"ALPHA"));
        }
        if let Some(d) = &self.store_destination {
            args.extend_from_slice(&[Bytes::from_static(b"STORE"), d.clone()]);
        }
        args
    }
}
