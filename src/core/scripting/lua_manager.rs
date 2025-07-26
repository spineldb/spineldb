// src/core/scripting/lua_manager.rs

use bytes::Bytes;
use dashmap::DashMap;
use sha1::{Digest, Sha1};
use std::collections::HashMap;

/// Manages the storage and retrieval of Lua scripts for EVALSHA.
#[derive(Debug, Default)]
pub struct LuaManager {
    /// A thread-safe hash map to store scripts, keyed by their SHA1 hash.
    scripts: DashMap<String, Bytes>,
}

impl LuaManager {
    /// Creates a new, empty Lua script manager.
    pub fn new() -> Self {
        Default::default()
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

    /// Removes all scripts from the cache.
    pub fn flush(&self) {
        self.scripts.clear();
    }
}
