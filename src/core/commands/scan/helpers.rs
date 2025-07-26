// src/core/commands/scan/helpers.rs

use crate::core::SpinelDBError;
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use bytes::Bytes;

/// The maximum recursion depth for glob pattern matching to prevent stack overflow.
const MAX_GLOB_RECURSION_DEPTH: u32 = 128;

/// Matches a string against a Redis-style glob pattern.
/// Supports `*`, `?`, `[...]`, `[^...]`, and `\`.
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    glob_match_recursive(pattern, string, 0)
}

/// The recursive implementation of `glob_match`.
fn glob_match_recursive(mut pattern: &[u8], mut string: &[u8], depth: u32) -> bool {
    if depth > MAX_GLOB_RECURSION_DEPTH {
        return false;
    }

    loop {
        match pattern.first() {
            // Empty pattern matches only an empty string.
            None => return string.is_empty(),
            // `*` matches any sequence of characters.
            Some(b'*') => {
                pattern = &pattern[1..];
                if pattern.is_empty() {
                    return true;
                }
                for i in 0..=string.len() {
                    if glob_match_recursive(pattern, &string[i..], depth + 1) {
                        return true;
                    }
                }
                return false;
            }
            // `?` matches any single character.
            Some(b'?') => {
                if string.is_empty() {
                    return false;
                }
                pattern = &pattern[1..];
                string = &string[1..];
            }
            // `[...]` matches any character in the set.
            Some(b'[') => {
                if string.is_empty() {
                    return false;
                }
                pattern = &pattern[1..];
                let (negated, p_rest) = if pattern.first() == Some(&b'^') {
                    (true, &pattern[1..])
                } else {
                    (false, pattern)
                };
                pattern = p_rest;
                let mut matched = false;
                let s_char = string[0];
                loop {
                    if pattern.is_empty() {
                        return false; // Unmatched bracket
                    }
                    if pattern.first() == Some(&b']') {
                        pattern = &pattern[1..];
                        break;
                    }
                    let p_start = pattern[0];
                    pattern = &pattern[1..];
                    if pattern.first() == Some(&b'-')
                        && !pattern[1..].is_empty()
                        && pattern[1] != b']'
                    {
                        let p_end = pattern[1];
                        pattern = &pattern[2..];
                        if s_char >= p_start && s_char <= p_end {
                            matched = true;
                        }
                    } else if s_char == p_start {
                        matched = true;
                    }
                }
                if negated {
                    matched = !matched;
                }
                if !matched {
                    return false;
                }
                string = &string[1..];
            }
            // `\` escapes the next character.
            Some(b'\\') => {
                pattern = &pattern[1..];
                if pattern.is_empty() || pattern.first() != string.first() {
                    return false;
                }
                pattern = &pattern[1..];
                string = &string[1..];
            }
            // A literal character must match exactly.
            Some(&p_char) => {
                if string.is_empty() || p_char != string[0] {
                    return false;
                }
                pattern = &pattern[1..];
                string = &string[1..];
            }
        }
    }
}

/// Parses common arguments for SCAN-family commands (cursor, MATCH, COUNT).
pub(super) fn parse_scan_args(
    args: &[RespFrame],
    min_args: usize,
    cmd_name: &str,
) -> Result<(u64, Option<Bytes>, Option<usize>), SpinelDBError> {
    if args.len() < min_args {
        return Err(SpinelDBError::WrongArgumentCount(cmd_name.to_string()));
    }
    let cursor = extract_string(&args[min_args - 1])?
        .parse::<u64>()
        .map_err(|_| SpinelDBError::SyntaxError)?;
    let mut pattern = None;
    let mut count = None;
    let mut i = min_args;
    while i < args.len() {
        let option = extract_string(&args[i])?.to_ascii_lowercase();
        match option.as_str() {
            "match" => {
                if i + 1 >= args.len() {
                    return Err(SpinelDBError::SyntaxError);
                }
                pattern = Some(extract_bytes(&args[i + 1])?);
                i += 2;
            }
            "count" => {
                if i + 1 >= args.len() {
                    return Err(SpinelDBError::SyntaxError);
                }
                count = Some(
                    extract_string(&args[i + 1])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?,
                );
                i += 2;
            }
            _ => return Err(SpinelDBError::SyntaxError),
        }
    }
    Ok((cursor, pattern, count))
}

/// Encodes a shard index and an internal cursor into a single u64 cursor.
///
/// The 8 most significant bits are used for the shard index, and the remaining
/// 56 bits are for the internal cursor. This allows for up to 256 shards.
pub fn encode_scan_cursor(shard_idx: usize, internal_cursor: usize) -> u64 {
    // Shift the shard index to the most significant bits.
    ((shard_idx as u64) << 56) | (internal_cursor as u64)
}

/// Decodes a u64 cursor into a shard index and an internal cursor.
pub fn decode_scan_cursor(cursor: u64) -> (usize, usize) {
    // Extract the shard index from the most significant bits.
    let shard_idx = (cursor >> 56) as usize;
    // Extract the internal cursor from the remaining bits.
    let internal_cursor = (cursor & 0x00FFFFFFFFFFFFFF) as usize;
    (shard_idx, internal_cursor)
}

/// Formats the optional MATCH and COUNT arguments back into a Vec<Bytes> for replication/AOF.
pub(super) fn format_scan_options_to_bytes(
    pattern: &Option<Bytes>,
    count: &Option<usize>,
) -> Vec<Bytes> {
    let mut args = Vec::new();
    if let Some(p) = pattern {
        args.push("MATCH".into());
        args.push(p.clone());
    }
    if let Some(c) = count {
        args.push("COUNT".into());
        args.push(c.to_string().into());
    }
    args
}
