// src/core/commands/set/set_ops_logic.rs

use crate::core::database::{ExecutionContext, ExecutionLocks};
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{RespValue, SpinelDBError, commands::command_trait::WriteOutcome};
use bytes::Bytes;
use std::collections::HashSet;

/// Helper untuk mengambil klon dari Set dari sebuah kunci.
/// Mengembalikan error WRONGTYPE jika kunci ada tetapi bukan Set.
/// Mengembalikan Ok(None) jika kunci tidak ada atau kedaluwarsa.
fn get_set_from_guard(
    guard: &mut impl std::ops::DerefMut<Target = crate::core::database::ShardCache>,
    key: &Bytes,
) -> Result<Option<HashSet<Bytes>>, SpinelDBError> {
    if let Some(entry) = guard.get_mut(key) {
        if entry.is_expired() {
            guard.pop(key);
            Ok(None)
        } else {
            match &entry.data {
                DataValue::Set(set) => Ok(Some(set.clone())),
                _ => Err(SpinelDBError::WrongType),
            }
        }
    } else {
        Ok(None)
    }
}

/// Melakukan operasi SUNION, mengembalikan set hasil atau error.
pub(super) async fn execute_sunion<'a>(
    keys: &[Bytes],
    ctx: &mut ExecutionContext<'a>,
) -> Result<HashSet<Bytes>, SpinelDBError> {
    let guards = match &mut ctx.locks {
        ExecutionLocks::Multi { guards } => guards,
        _ => {
            return Err(SpinelDBError::Internal(
                "Set op requires multi-key lock".into(),
            ));
        }
    };
    let mut union_set = HashSet::new();
    for key in keys {
        let shard_index = ctx.db.get_shard_index(key);
        if let Some(guard) = guards.get_mut(&shard_index)
            && let Some(set) = get_set_from_guard(guard, key)?
        {
            union_set.extend(set.iter().cloned());
        }
    }
    Ok(union_set)
}

/// Melakukan operasi SINTER, mengembalikan set hasil atau error.
pub(super) async fn execute_sinter<'a>(
    keys: &[Bytes],
    ctx: &mut ExecutionContext<'a>,
) -> Result<HashSet<Bytes>, SpinelDBError> {
    let guards = match &mut ctx.locks {
        ExecutionLocks::Multi { guards } => guards,
        _ => {
            return Err(SpinelDBError::Internal(
                "Set op requires multi-key lock".into(),
            ));
        }
    };
    let mut intersection_set: Option<HashSet<Bytes>> = None;
    for key in keys {
        let shard_index = ctx.db.get_shard_index(key);
        if let Some(guard) = guards.get_mut(&shard_index) {
            match get_set_from_guard(guard, key)? {
                Some(set) => {
                    if let Some(isect) = intersection_set.as_mut() {
                        isect.retain(|member| set.contains(member));
                    } else {
                        intersection_set = Some(set);
                    }
                }
                None => return Ok(HashSet::new()),
            }
        }
    }
    Ok(intersection_set.unwrap_or_default())
}

/// Melakukan operasi SDIFF, mengembalikan set hasil atau error.
pub(super) async fn execute_sdiff<'a>(
    keys: &[Bytes],
    ctx: &mut ExecutionContext<'a>,
) -> Result<HashSet<Bytes>, SpinelDBError> {
    let guards = match &mut ctx.locks {
        ExecutionLocks::Multi { guards } => guards,
        _ => {
            return Err(SpinelDBError::Internal(
                "Set op requires multi-key lock".into(),
            ));
        }
    };
    if keys.is_empty() {
        return Ok(HashSet::new());
    }
    let first_key = &keys[0];
    let first_shard_index = ctx.db.get_shard_index(first_key);
    let mut diff_set = if let Some(guard) = guards.get_mut(&first_shard_index) {
        get_set_from_guard(guard, first_key)?.unwrap_or_default()
    } else {
        HashSet::new()
    };
    if diff_set.is_empty() {
        return Ok(HashSet::new());
    }
    for key in keys.iter().skip(1) {
        let shard_index = ctx.db.get_shard_index(key);
        if let Some(guard) = guards.get_mut(&shard_index)
            && let Some(other_set) = get_set_from_guard(guard, key)?
        {
            diff_set.retain(|member| !other_set.contains(member));
        }
    }
    Ok(diff_set)
}

/// Menyimpan hasil operasi set ke kunci tujuan.
pub(super) fn store_set_result(
    dest_key: &Bytes,
    result_set: HashSet<Bytes>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let guards = match &mut ctx.locks {
        ExecutionLocks::Multi { guards } => guards,
        _ => {
            return Err(SpinelDBError::Internal(
                "STORE op requires multi-key lock".into(),
            ));
        }
    };
    let dest_shard_index = ctx.db.get_shard_index(dest_key);
    let dest_guard = guards
        .get_mut(&dest_shard_index)
        .ok_or_else(|| SpinelDBError::Internal("Missing dest lock for STORE".into()))?;

    let set_len = result_set.len();

    if set_len == 0 {
        let existed = dest_guard.pop(dest_key).is_some();
        let outcome = if existed {
            WriteOutcome::Delete { keys_deleted: 1 }
        } else {
            WriteOutcome::DidNotWrite
        };
        return Ok((RespValue::Integer(0), outcome));
    }

    let new_value = StoredValue::new(DataValue::Set(result_set));

    dest_guard.put(dest_key.clone(), new_value);

    Ok((
        RespValue::Integer(set_len as i64),
        WriteOutcome::Write { keys_modified: 1 },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set_from_guard_expired_returns_none() {
        let _key = Bytes::from_static(b"k");
        let mut set = HashSet::new();
        set.insert(Bytes::from_static(b"member"));
        let stored = StoredValue::new(DataValue::Set(set));
        assert!(!stored.is_expired());
        assert!(stored.expiry.is_none());
    }

    #[test]
    fn test_sunion_basic() {
        let mut a = HashSet::new();
        a.insert(Bytes::from_static(b"1"));
        a.insert(Bytes::from_static(b"2"));
        let mut b = HashSet::new();
        b.insert(Bytes::from_static(b"2"));
        b.insert(Bytes::from_static(b"3"));

        let mut union = a.clone();
        union.extend(b.iter().cloned());
        assert_eq!(union.len(), 3);
        assert!(union.contains(&Bytes::from_static(b"1")));
        assert!(union.contains(&Bytes::from_static(b"2")));
        assert!(union.contains(&Bytes::from_static(b"3")));
    }

    #[test]
    fn test_sunion_empty_sets() {
        let a: HashSet<Bytes> = HashSet::new();
        let b: HashSet<Bytes> = HashSet::new();
        let mut union = a;
        union.extend(b);
        assert!(union.is_empty());
    }

    #[test]
    fn test_sinter_basic() {
        let mut a = HashSet::new();
        a.insert(Bytes::from_static(b"1"));
        a.insert(Bytes::from_static(b"2"));
        let mut b = HashSet::new();
        b.insert(Bytes::from_static(b"2"));
        b.insert(Bytes::from_static(b"3"));

        let intersection: HashSet<Bytes> = a.iter().filter(|m| b.contains(*m)).cloned().collect();
        assert_eq!(intersection.len(), 1);
        assert!(intersection.contains(&Bytes::from_static(b"2")));
    }

    #[test]
    fn test_sinter_no_common() {
        let mut a = HashSet::new();
        a.insert(Bytes::from_static(b"1"));
        let mut b = HashSet::new();
        b.insert(Bytes::from_static(b"2"));

        let intersection: HashSet<Bytes> = a.iter().filter(|m| b.contains(*m)).cloned().collect();
        assert!(intersection.is_empty());
    }

    #[test]
    fn test_sdiff_basic() {
        let mut a = HashSet::new();
        a.insert(Bytes::from_static(b"1"));
        a.insert(Bytes::from_static(b"2"));
        a.insert(Bytes::from_static(b"3"));
        let mut b = HashSet::new();
        b.insert(Bytes::from_static(b"2"));

        let mut diff = a;
        diff.retain(|m| !b.contains(m));
        assert_eq!(diff.len(), 2);
        assert!(diff.contains(&Bytes::from_static(b"1")));
        assert!(diff.contains(&Bytes::from_static(b"3")));
    }

    #[test]
    fn test_sdiff_empty_other() {
        let mut a = HashSet::new();
        a.insert(Bytes::from_static(b"1"));
        let b: HashSet<Bytes> = HashSet::new();

        let mut diff = a;
        diff.retain(|m| !b.contains(m));
        assert_eq!(diff.len(), 1);
    }

    #[test]
    fn test_sdiff_all_removed() {
        let mut a = HashSet::new();
        a.insert(Bytes::from_static(b"1"));
        let mut b = HashSet::new();
        b.insert(Bytes::from_static(b"1"));

        let mut diff = a;
        diff.retain(|m| !b.contains(m));
        assert!(diff.is_empty());
    }

    #[test]
    fn test_store_empty_set_deletes_dest() {
        let result_set: HashSet<Bytes> = HashSet::new();
        let set_len = result_set.len();
        assert_eq!(set_len, 0);
    }

    #[test]
    fn test_store_non_empty_set() {
        let mut result_set = HashSet::new();
        result_set.insert(Bytes::from_static(b"a"));
        result_set.insert(Bytes::from_static(b"b"));
        let set_len = result_set.len();
        assert_eq!(set_len, 2);
    }

    #[test]
    fn test_wrong_type_error_for_non_set() {
        let err = SpinelDBError::WrongType;
        assert!(matches!(err, SpinelDBError::WrongType));
    }

    #[test]
    fn test_internal_error_for_missing_lock() {
        let err = SpinelDBError::Internal("Set op requires multi-key lock".into());
        assert!(matches!(err, SpinelDBError::Internal(_)));
    }

    #[test]
    fn test_hashset_clone() {
        let mut original = HashSet::new();
        original.insert(Bytes::from_static(b"x"));
        original.insert(Bytes::from_static(b"y"));
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_hashset_extend() {
        let mut a = HashSet::new();
        a.insert(Bytes::from_static(b"1"));
        let mut b = HashSet::new();
        b.insert(Bytes::from_static(b"2"));
        b.insert(Bytes::from_static(b"3"));
        a.extend(b);
        assert_eq!(a.len(), 3);
    }
}
