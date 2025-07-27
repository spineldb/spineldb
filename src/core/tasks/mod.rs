// src/core/tasks/mod.rs

//! This module contains all long-running background tasks that support the
//! server's core functionality, such as maintenance, persistence, and caching.

pub mod cache_gc;
pub mod cache_lock_cleaner;
pub mod cache_purger;
pub mod cache_revalidator;
pub mod cache_tag_validator;
pub mod eviction;
pub mod lazy_free;
pub mod persistence;
pub mod replica_quorum_validator;
