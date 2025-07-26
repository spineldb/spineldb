// src/core/commands/key_extractor.rs

//! Centralized key extraction logic for ACLs and cluster routing.
//! This module maps command names to their specific key extraction patterns.

use crate::core::SpinelDBError;
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use bytes::Bytes;

/// Extracts keys from a command's arguments based on its name.
/// This is the main dispatch function used by the router and ACL enforcer.
pub fn extract_keys_from_command(
    command_name: &str,
    args: &[RespFrame],
) -> Result<Vec<Bytes>, SpinelDBError> {
    // Match on the lowercase command name for consistency.
    let lower_cmd = command_name.to_ascii_lowercase();
    match lower_cmd.as_str() {
        // --- Special handling for namespaced commands ---
        s if s.starts_with("json.") => {
            // JSON.MGET has a unique key format: key1 key2 ... path
            if s == "json.mget" {
                if args.len() < 2 {
                    return Err(SpinelDBError::SyntaxError);
                }
                // All args except the last one are keys.
                return args[..args.len() - 1].iter().map(extract_bytes).collect();
            }
            // For all other implemented JSON.* commands, the key is the first argument.
            extract_n_keys(args, 1, 1, 1)
        }
        s if s.starts_with("cache.") => {
            // For most CACHE.* commands, the key is the first argument.
            // Subcommands without keys (like STATS, PURGETAG, POLICY) will correctly return an empty Vec.
            extract_n_keys(args, 1, 1, 1)
        }

        // --- Commands with a single key at position 0 ---
        "get" | "set" | "del" | "unlink" | "incr" | "decr" | "append" | "strlen" | "getdel"
        | "getex" | "getset" | "lpush" | "rpush" | "lpop" | "rpop" | "llen" | "ltrim"
        | "lindex" | "lset" | "sadd" | "smembers" | "scard" | "srem" | "sismember" | "spop"
        | "srandmember" | "smismember" | "hgetall" | "hkeys" | "hvals" | "hlen" | "hdel"
        | "hget" | "hexists" | "hstrlen" | "hset" | "hsetnx" | "hmget" | "hincrby"
        | "hincrbyfloat" | "hrandfield" | "zadd" | "zcard" | "zscore" | "zrank" | "zrevrank"
        | "zrem" | "zincrby" | "zpopmin" | "zpopmax" | "zmscore" | "xadd" | "xlen" | "xdel"
        | "xtrim" | "xinfo" | "expire" | "pexpire" | "expireat" | "pexpireat" | "ttl" | "pttl"
        | "persist" | "type" | "dump" | "restore" | "bitfield" | "bitcount" | "bitpos"
        | "getbit" | "setbit" | "linsert" | "lpos" | "lrem" | "zcount" | "zlexcount"
        | "zremrangebylex" | "zremrangebyrank" | "zremrangebyscore" | "zrangebylex"
        | "zrangebyscore" | "xack" | "xclaim" | "xgroup" | "xpending" | "xread" | "xreadgroup"
        | "xautoclaim" | "geoadd" | "geopos" | "geodist" | "georadius" | "georadiusbymember"
        | "setex" | "psetex" | "lpushx" | "rpushx" => extract_n_keys(args, 1, 1, 1),

        // --- Commands with keys from position 0 to N ---
        "mget" | "exists" | "sdiff" | "sinter" | "sunion" | "bzpopmin" | "bzpopmax" | "blpop"
        | "brpop" => extract_up_to_n_keys(args, args.len()),

        // --- Commands with keys at pos 0 and 1 ---
        "rename" | "renamenx" | "smove" | "lmove" | "blmove" => extract_n_keys(args, 2, 1, 1),

        // --- Commands with complex key specifications ---
        "mset" => extract_by_step(args, 1, 2),
        "msetnx" => extract_by_step(args, 1, 2),

        "zunionstore" | "zinterstore" => extract_store_op_keys(args),
        "sdiffstore" | "sinterstore" | "sunionstore" => extract_store_op_keys(args),

        "zrangestore" => extract_n_keys(args, 2, 1, 1),

        "bitop" => extract_bitop_keys(args),
        "migrate" => extract_migrate_keys(args),

        _ => Ok(vec![]),
    }
}

/// Extracts a fixed number of keys starting from the first argument.
fn extract_n_keys(
    args: &[RespFrame],
    num_keys: usize,
    _first: usize,
    _step: usize,
) -> Result<Vec<Bytes>, SpinelDBError> {
    if args.len() < num_keys {
        return Err(SpinelDBError::SyntaxError);
    }
    args[..num_keys].iter().map(extract_bytes).collect()
}

/// Extracts all arguments as keys. Used for variadic key commands.
fn extract_up_to_n_keys(args: &[RespFrame], num_keys: usize) -> Result<Vec<Bytes>, SpinelDBError> {
    if args.len() < num_keys {
        return Err(SpinelDBError::SyntaxError);
    }
    args[..num_keys].iter().map(extract_bytes).collect()
}

/// Extracts keys that appear at a specific interval (e.g., MSET key val key val...).
fn extract_by_step(
    args: &[RespFrame],
    _first: usize,
    step: usize,
) -> Result<Vec<Bytes>, SpinelDBError> {
    args.iter().step_by(step).map(extract_bytes).collect()
}

/// Extracts keys for ZUNIONSTORE/ZINTERSTORE/etc. format: dest numkeys key1 key2 ...
fn extract_store_op_keys(args: &[RespFrame]) -> Result<Vec<Bytes>, SpinelDBError> {
    if args.len() < 2 {
        return Err(SpinelDBError::SyntaxError);
    }
    let mut keys = Vec::with_capacity(16);
    // Destination key is always the first argument.
    keys.push(extract_bytes(&args[0])?);

    // Parse numkeys to know how many source keys to read.
    let num_keys: usize = extract_string(&args[1])?.parse()?;
    if args.len() < 2 + num_keys {
        return Err(SpinelDBError::SyntaxError);
    }

    args.iter()
        .skip(2)
        .take(num_keys)
        .map(extract_bytes)
        .try_for_each(|key_result| -> Result<(), SpinelDBError> {
            keys.push(key_result?);
            Ok(())
        })?;

    Ok(keys)
}

/// Extracts all keys for a BITOP command: dest_key src_key [src_key ...].
fn extract_bitop_keys(args: &[RespFrame]) -> Result<Vec<Bytes>, SpinelDBError> {
    if args.len() < 2 {
        return Err(SpinelDBError::SyntaxError);
    }
    // All arguments from the second onwards are keys.
    args.iter().skip(1).map(extract_bytes).collect()
}

/// Extracts the single key from a MIGRATE command.
fn extract_migrate_keys(args: &[RespFrame]) -> Result<Vec<Bytes>, SpinelDBError> {
    if args.len() < 5 {
        return Err(SpinelDBError::WrongArgumentCount("MIGRATE".to_string()));
    }
    // The key is the 3rd argument (index 2).
    Ok(vec![extract_bytes(&args[2])?])
}
