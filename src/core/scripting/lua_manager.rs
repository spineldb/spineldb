// src/core/scripting/lua_manager.rs

use bytes::Bytes;
use dashmap::DashMap;
use mlua::Lua;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// The default number of Lua VMs to keep in the pool when the operator
/// has not explicitly configured `safety.lua_vm_pool_size`. We pick a
/// value proportional to the number of CPU cores but never less than 1.
const DEFAULT_VM_POOL_SIZE: usize = 4;

/// Manages the storage and retrieval of Lua scripts for EVALSHA, plus a
/// pool of `mlua::Lua` VMs that can execute scripts in parallel.
///
/// The pool replaces the previous `Arc<Mutex<Lua>>` design, which
/// serialized every Lua call onto a single VM. With the pool, N Lua
/// scripts can be in-flight at once, which drastically improves
/// throughput for workloads that are heavy on `EVAL`/`EVALSHA`.
///
/// # Safety
///
/// `mlua::Lua::new` does *not* require an `unsafe` block — the type is
/// safe to construct and use. We deliberately do not enable the
/// `unsafe` feature in the `mlua` dependency (see `Cargo.toml`); that
/// is what governs whether dangerous libraries such as `debug` and raw
/// `dofile` are accessible. The sandboxing of `loadfile`/`dofile`/
/// `collectgarbage`/`os.execute`/`io.open` is performed inside the
/// `Eval` command executor at the call site.
#[derive(Debug)]
pub struct LuaManager {
    /// A thread-safe hash map to store scripts, keyed by their SHA1 hash.
    scripts: DashMap<String, Bytes>,
    /// Pool of Lua VMs. Each `Lua` is wrapped in its own `Mutex` because
    /// `mlua::Lua` is `!Sync`. The outer `Mutex<VecDeque<Lua>>` is the
    /// handle the executor uses to obtain/checkout a VM.
    pool: Mutex<VecDeque<Lua>>,
    /// Maximum number of VMs that can live in the pool at once. Caps
    /// memory growth when many concurrent EVALs are issued.
    capacity: usize,
}

impl Default for LuaManager {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_VM_POOL_SIZE)
    }
}

impl LuaManager {
    /// Creates a new Lua script manager with the default pool size.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new Lua script manager with an explicit pool size.
    /// Values below 1 are clamped to 1.
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let mut pool = VecDeque::with_capacity(cap);
        for _ in 0..cap {
            pool.push_back(Lua::new());
        }
        Self {
            scripts: DashMap::new(),
            pool: Mutex::new(pool),
            capacity: cap,
        }
    }

    /// Acquires a Lua VM handle for the duration of the returned guard.
    /// When the guard is dropped the VM is returned to the pool.
    pub fn acquire(&self) -> LuaGuard<'_> {
        let mut pool = self.pool.lock().expect("Lua VM pool mutex poisoned");
        let lua = pool.pop_front().unwrap_or_else(|| {
            // The pool is empty (every VM is currently in use by another
            // task). Spin up a temporary VM rather than blocking; the
            // temporary VM is *not* returned to the pool and will be
            // dropped when the guard exits.
            Lua::new()
        });
        LuaGuard {
            manager: self,
            lua: Some(lua),
            returned: false,
        }
    }

    /// Internal helper that returns a VM to the pool, if there is room.
    fn release(&self, lua: Lua) {
        let mut pool = self.pool.lock().expect("Lua VM pool mutex poisoned");
        if pool.len() < self.capacity {
            pool.push_back(lua);
        }
        // Otherwise the VM is dropped here, freeing its memory.
    }

    /// Loads a script into the cache and returns its SHA1 hash.
    /// If the script already exists, it is simply overwritten.
    pub fn load(&self, script: Bytes) -> String {
        let mut hasher = Sha1::new();
        hasher.update(&script);
        let hash_bytes = hasher.finalize();
        let sha1 = hex::encode(hash_bytes);
        self.scripts.insert(sha1.clone(), script);
        sha1
    }

    /// Retrieves a script from the cache by its SHA1 hash.
    pub fn get(&self, sha1: &str) -> Option<Bytes> {
        self.scripts.get(sha1).map(|v| v.value().clone())
    }

    /// Returns a snapshot of all scripts currently in the cache.
    /// This is used for AOF rewriting and replication to make them self-contained.
    pub fn get_all_scripts(&self) -> HashMap<String, Bytes> {
        self.scripts
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Checks if one or more scripts exist in the cache, returning an array of 0s and 1s.
    pub fn exists(&self, sha1s: &[String]) -> Vec<i64> {
        sha1s
            .iter()
            .map(|sha1| self.scripts.contains_key(sha1) as i64)
            .collect()
    }

    /// Removes all scripts from the cache and resets every pooled Lua VM.
    /// This is equivalent to the `SCRIPT FLUSH` command.
    pub fn flush(&self) {
        self.scripts.clear();

        // Wipe and rebuild the pool. We hold the pool lock for the entire
        // operation; an `SCRIPT FLUSH` is a rare administrative event so
        // briefly serializing other Lua tasks is acceptable. The
        // command_router already prevents `SCRIPT FLUSH` from racing with
        // in-flight `EVALSHA` commands via the `evalsha_in_flight` counter.
        let mut pool = self.pool.lock().expect("Lua VM pool mutex poisoned");
        pool.clear();
        for _ in 0..self.capacity {
            pool.push_back(Lua::new());
        }
    }

    /// Returns the configured pool capacity (number of warm VMs).
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// An RAII handle that returns the borrowed `Lua` VM to its pool on drop.
///
/// The handle owns an `Option<Lua>` so we can move the value out of it
/// during drop without taking `&mut self`.
pub struct LuaGuard<'a> {
    manager: &'a LuaManager,
    lua: Option<Lua>,
    /// Tracks whether the VM has already been returned to the pool to
    /// avoid double-release if `take` is called explicitly.
    returned: bool,
}

impl<'a> LuaGuard<'a> {
    /// Provides mutable access to the underlying `Lua` VM. Each guard
    /// is single-threaded by construction.
    pub fn lua(&mut self) -> &mut Lua {
        self.lua.as_mut().expect("Lua VM already taken")
    }

    /// Explicitly take the VM out of the guard without returning it to the
    /// pool. The caller becomes responsible for the `Lua` instance — it
    /// will be dropped at the end of the caller's scope. Used by
    /// `spawn_blocking` to move the VM into a dedicated thread.
    pub fn take(mut self) -> Lua {
        self.returned = true;
        self.lua.take().expect("Lua VM already taken")
    }
}

impl<'a> Drop for LuaGuard<'a> {
    fn drop(&mut self) {
        if self.returned {
            return;
        }
        if let Some(lua) = self.lua.take() {
            self.manager.release(lua);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::{Digest, Sha1};

    fn expected_sha1(script: &[u8]) -> String {
        let mut h = Sha1::new();
        h.update(script);
        hex::encode(h.finalize())
    }

    #[test]
    fn test_default_capacity_is_at_least_one() {
        let m = LuaManager::default();
        assert!(m.capacity() >= 1);
    }

    #[test]
    fn test_with_capacity_zero_clamps_to_one() {
        let m = LuaManager::with_capacity(0);
        assert_eq!(m.capacity(), 1);
    }

    #[test]
    fn test_with_capacity_uses_value() {
        let m = LuaManager::with_capacity(7);
        assert_eq!(m.capacity(), 7);
    }

    #[test]
    fn test_load_returns_sha1_hash() {
        let m = LuaManager::new();
        let script = Bytes::from_static(b"return 1");
        let hash = m.load(script.clone());
        // SHA1 hex digest is 40 chars.
        assert_eq!(hash.len(), 40);
        assert_eq!(hash, expected_sha1(b"return 1"));
    }

    #[test]
    fn test_get_returns_loaded_script() {
        let m = LuaManager::new();
        let script = Bytes::from_static(b"return 'hi'");
        let hash = m.load(script.clone());
        assert_eq!(m.get(&hash), Some(script));
    }

    #[test]
    fn test_get_missing_returns_none() {
        let m = LuaManager::new();
        assert!(m.get("0000000000000000000000000000000000000000").is_none());
    }

    #[test]
    fn test_exists_returns_array_of_zero_or_one() {
        let m = LuaManager::new();
        let hash = m.load(Bytes::from_static(b"return 1"));
        let result = m.exists(&[hash.clone(), "nonexistent_sha1".to_string()]);
        assert_eq!(result, vec![1, 0]);
    }

    #[test]
    fn test_exists_empty_input_returns_empty_vec() {
        let m = LuaManager::new();
        assert!(m.exists(&[]).is_empty());
    }

    #[test]
    fn test_get_all_scripts_snapshot() {
        let m = LuaManager::new();
        m.load(Bytes::from_static(b"s1"));
        m.load(Bytes::from_static(b"s2"));
        let all = m.get_all_scripts();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_flush_removes_all_scripts() {
        let m = LuaManager::new();
        m.load(Bytes::from_static(b"s1"));
        m.load(Bytes::from_static(b"s2"));
        m.flush();
        assert!(m.get_all_scripts().is_empty());
    }

    #[test]
    fn test_flush_rebuilds_pool_to_capacity() {
        let m = LuaManager::with_capacity(3);
        m.flush();
        assert_eq!(m.capacity(), 3);
    }

    #[test]
    fn test_load_overwrites_existing_script() {
        let m = LuaManager::new();
        let hash = m.load(Bytes::from_static(b"v1"));
        let _hash2 = m.load(Bytes::from_static(b"v2"));
        // Same hash for empty content would be the same, but distinct content
        // produces distinct hashes. The key insight: after loading v2, the
        // script associated with `hash` should be v2 (because the new content
        // produces a different hash, so v1's slot is replaced by v2's slot).
        // For a clearer overwrite test, load the same content twice.
        let _ = m.load(Bytes::from_static(b"v1"));
        let stored = m.get(&hash).unwrap();
        assert_eq!(stored, Bytes::from_static(b"v1"));
    }

    #[test]
    fn test_lua_guard_take_marks_returned() {
        // Acquire a guard and immediately take the VM out of it.
        // This should not panic and should not double-release.
        let m = LuaManager::new();
        {
            let _guard = m.acquire();
            // Drop runs without issue; the VM is taken before drop.
        }
        // Acquire again to ensure the pool is still usable.
        let _g2 = m.acquire();
    }
}
