// src/core/commands/mod.rs

//! This module defines all supported commands, organizes them into categories,
//! and provides the central `Command` enum that encapsulates their parsed state.
//! The `define_commands!` macro is used to generate the enum and its core
//! implementations, reducing boilerplate and ensuring consistency.

use crate::core::commands::command_trait::{
    CommandExt, CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::database::ExecutionContext;
use crate::core::handler::command_router::RouteResponse;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

// Re-export the CommandSpec trait for easy access by other modules.
pub use command_spec::CommandSpec;

// The macro definition that generates the Command enum and its implementations.
#[macro_use]
mod command_def;

// Publicly declare all command category modules.
pub mod cache;
pub mod cluster;
pub mod command_spec;
pub mod command_trait;
pub mod generic;
pub mod geospatial;
pub mod hash;
pub mod helpers;
pub mod hyperloglog;
pub mod json;
pub mod key_extractor;
pub mod list;
pub mod scan;
pub mod set;
pub mod streams;
pub mod string;
pub mod zset;

// Use the macro to define all supported commands.
// Dispatchers handle namespaced commands (e.g., `CACHE.GET`), while standard
// commands are top-level (e.g., `GET`).
define_commands! {
    dispatchers: {
        (Cache, Cache, cache),
        (Cluster, ClusterInfo, cluster),
        (Json, Json, json)
    },
    standard: {
        // --- Generic Commands ---
        (Command, CommandInfo, generic),
        (Config, ConfigGetSet, generic),
        (Type, TypeInfo, generic),
        (PubSub, PubSubInfo, generic),
        (Ping, Ping, generic),
        (Echo, Echo, generic),
        (Auth, Auth, generic),
        (Select, Select, generic),
        (Quit, Quit, generic),
        (Shutdown, Shutdown, generic),
        (Del, Del, generic),
        (Unlink, Unlink, generic),
        (FlushAll, FlushAll, generic),
        (FlushDb, FlushDb, generic),
        (Expire, Expire, generic),
        (ExpireAt, ExpireAt, generic),
        (PExpire, PExpire, generic),
        (PExpireAt, PExpireAt, generic),
        (Ttl, Ttl, generic),
        (Pttl, Pttl, generic),
        (Persist, Persist, generic),
        (Keys, Keys, generic),
        (DbSize, DbSize, generic),
        (Publish, Publish, generic),
        (Subscribe, Subscribe, generic),
        (PSubscribe, PSubscribe, generic),
        (Unsubscribe, Unsubscribe, generic),
        (PUnsubscribe, PUnsubscribe, generic),
        (Watch, Watch, generic),
        (Unwatch, Unwatch, generic),
        (Replconf, Replconf, generic),
        (Psync, Psync, generic),
        (Info, Info, generic),
        (Asking, Asking, generic),
        (BgRewriteAof, BgRewriteAof, generic),
        (Sort, Sort, generic),
        (Exists, Exists, generic),
        (Rename, Rename, generic),
        (RenameNx, RenameNx, generic),
        (Save, Save, generic),
        (BgSave, BgSave, generic),
        (Backup, Backup, generic),
        (Client, Client, generic),
        (Time, Time, generic),
        (Role, Role, generic),
        (LastSave, LastSave, generic),
        (Slowlog, Slowlog, generic),
        (Memory, Memory, generic),
        (Latency, Latency, generic),
        (Migrate, Migrate, generic),
        (Restore, Restore, generic),
        (Script, Script, generic),
        (Eval, Eval, generic),
        (EvalSha, EvalSha, generic),
        (Acl, Acl, generic),
        (Failover, Failover, generic),

        // --- String Commands ---
        (Get, Get, string),
        (Set, Set, string),
        (SetEx, SetEx, string),
        (PSetEx, PSetEx, string),
        (GetRange, GetRange, string),
        (SetRange, SetRange, string),
        (Append, Append, string),
        (Incr, Incr, string),
        (Decr, Decr, string),
        (IncrBy, IncrBy, string),
        (IncrByFloat, IncrByFloat, string),
        (DecrBy, DecrBy, string),
        (MGet, MGet, string),
        (MSet, MSet, string),
        (MSetNx, MSetNx, string),
        (StrLen, StrLen, string),
        (SetBit, SetBit, string),
        (GetBit, GetBit, string),
        (BitCount, BitCount, string),
        (BitOp, BitOp, string),
        (BitPos, BitPos, string),
        (GetDel, GetDel, string),
        (GetEx, GetEx, string),
        (GetSet, GetSet, string),
        (BitField, BitField, string),

        // --- List Commands ---
        (LPush, LPush, list),
        (LPushX, LPushX, list),
        (RPush, RPush, list),
        (RPushX, RPushX, list),
        (LPop, LPop, list),
        (RPop, RPop, list),
        (LMove, LMove, list),
        (BLMove, BLMove, list),
        (LLen, LLen, list),
        (LRange, LRange, list),
        (LIndex, LIndex, list),
        (LTrim, LTrim, list),
        (LInsert, LInsert, list),
        (LSet, LSet, list),
        (LPos, LPos, list),
        (BLPop, BLPop, list),
        (BRPop, BRPop, list),
        (LRem, LRem, list),

        // --- Hash Commands ---
        (HSet, HSet, hash),
        (HGet, HGet, hash),
        (HGetAll, HGetAll, hash),
        (HDel, HDel, hash),
        (HExists, HExists, hash),
        (HLen, HLen, hash),
        (HKeys, HKeys, hash),
        (HVals, HVals, hash),
        (HIncrBy, HIncrBy, hash),
        (HIncrByFloat, HIncrByFloat, hash),
        (HmGet, HmGet, hash),
        (HRandField, HRandField, hash),
        (HSetNx, HSetNx, hash),
        (HStrLen, HStrLen, hash),

        // --- Set Commands ---
        (Sadd, Sadd, set),
        (Srem, Srem, set),
        (Smembers, Smembers, set),
        (Sismember, Sismember, set),
        (Scard, Scard, set),
        (SPop, SPop, set),
        (SUnion, SUnion, set),
        (SInter, SInter, set),
        (Sdiff, Sdiff, set),
        (SrandMember, SrandMember, set),
        (Smove, Smove, set),
        (SMIsMember, SMIsMember, set),
        (SUnionStore, SUnionStore, set),
        (SInterStore, SInterStore, set),
        (SdiffStore, SdiffStore, set),

        // --- Sorted Set Commands ---
        (Zadd, Zadd, zset),
        (ZRange, ZRange, zset),
        (ZRangeByScore, ZRangeByScore, zset),
        (ZRangeByLex, ZRangeByLex, zset),
        (ZRevRange, ZRevRange, zset),
        (ZCard, ZCard, zset),
        (ZScore, ZScore, zset),
        (ZCount, ZCount, zset),
        (ZLexCount, ZLexCount, zset),
        (ZRem, ZRem, zset),
        (ZRemRangeByScore, ZRemRangeByScore, zset),
        (ZRank, ZRank, zset),
        (ZRevRank, ZRevRank, zset),
        (ZIncrBy, ZIncrBy, zset),
        (ZPopMin, ZPopMin, zset),
        (ZPopMax, ZPopMax, zset),
        (BZPopMin, BZPopMin, zset),
        (BZPopMax, BZPopMax, zset),
        (ZUnionStore, ZUnionStore, zset),
        (ZInterStore, ZInterStore, zset),
        (ZRemRangeByLex, ZRemRangeByLex, zset),
        (ZRemRangeByRank, ZRemRangeByRank, zset),
        (ZRangeStore, ZRangeStore, zset),
        (ZMScore, ZMScore, zset),

        // --- Geospatial Commands ---
        (GeoAdd, GeoAdd, geospatial),
        (GeoPos, GeoPos, geospatial),
        (GeoDist, GeoDist, geospatial),
        (GeoRadius, GeoRadiusCmd, geospatial),
        (GeoRadiusByMember, GeoRadiusByMemberCmd, geospatial),

        // --- Stream Commands ---
        (XAdd, XAdd, streams),
        (XRange, XRange, streams),
        (XRevRange, XRevRange, streams),
        (XTrim, XTrim, streams),
        (XDel, XDel, streams),
        (XLen, XLen, streams),
        (XInfo, XInfo, streams),
        (XGroup, XGroup, streams),
        (XAck, XAck, streams),
        (XPending, XPending, streams),
        (XClaim, XClaim, streams),
        (XRead, XRead, streams),
        (XReadGroup, XReadGroup, streams),
        (XAutoClaim, XAutoClaim, streams),

        // --- Scan Commands ---
        (Scan, Scan, scan),
        (HScan, HScan, scan),
        (SScan, SScan, scan),
        (ZScan, ZScan, scan),

        // --- HyperLogLog Commands ---
        (PfAdd, PfAdd, hyperloglog),
        (PfCount, PfCount, hyperloglog),
        (PfMerge, PfMerge, hyperloglog)
    }
}
