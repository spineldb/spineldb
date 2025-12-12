// src/core/scripting/lua_manager.rs

use bytes::Bytes;
use dashmap::DashMap;
use mlua::Lua;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::sync::Mutex;

/// Manages the storage and retrieval of Lua scripts for EVALSHA.
#[derive(Debug)]
pub struct LuaManager {
    /// A thread-safe hash map to store scripts, keyed by their SHA1 hash.
    scripts: DashMap<String, Bytes>,
    /// A persistent Lua VM instance used for script execution.
    /// Wrapped in a Mutex because mlua::Lua is not Sync.
    pub vm: Mutex<Lua>,
}

impl Default for LuaManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LuaManager {
    /// Creates a new Lua script manager with a pre-initialized Lua VM.
    pub fn new() -> Self {
        // This is marked as unsafe because it allows the loading of potentially dangerous
        // libraries like 'debug'. In our controlled environment, we accept this risk
        // to provide full-featured scripting capabilities.
        let lua = Lua::new();

        Self {
            scripts: DashMap::new(),
            vm: Mutex::new(lua),
        }
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

    /// Removes all scripts from the cache and resets the Lua VM.
    /// This is equivalent to the `SCRIPT FLUSH` command.
    pub fn flush(&self) {
        self.scripts.clear();

        // Re-initialize the VM to clear any global state set by previous scripts.
        if let Ok(mut vm_guard) = self.vm.lock() {
            // This is marked as unsafe for the same reasons as in the `new` function.
            let new_lua = Lua::new();
            *vm_guard = new_lua;
        }
    }
}
