// src/core/commands/scan/helpers.rs

use crate::core::SpinelDBError;
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use bytes::Bytes;

/// Matches a string against a Redis-style glob pattern.
/// Supports `*`, `?`, `[...]`, `[^...]`, and `\`.
/// This implementation is iterative to prevent stack overflow from complex patterns.
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut p_idx = 0;
    let mut s_idx = 0;
    let mut star_p_idx = None; // Index in pattern after the last '*'
    let mut star_s_idx = None; // Index in string to backtrack to on mismatch

    while s_idx < string.len() {
        match pattern.get(p_idx) {
            // Match a single character
            Some(b'?') => {
                p_idx += 1;
                s_idx += 1;
            }
            // Star wildcard: save backtrack position and advance pattern
            Some(b'*') => {
                star_p_idx = Some(p_idx + 1);
                star_s_idx = Some(s_idx);
                p_idx += 1;
            }
            // Character set match
            Some(b'[') => {
                match parse_char_set(&pattern[p_idx..], string[s_idx]) {
                    Some(len) => {
                        p_idx += len;
                        s_idx += 1;
                    }
                    None => {
                        // Mismatch, try backtracking to the last star
                        if let (Some(p), Some(s)) = (star_p_idx, star_s_idx) {
                            p_idx = p;
                            s_idx = s + 1;
                            star_s_idx = Some(s + 1);
                        } else {
                            return false;
                        }
                    }
                }
            }
            // Escaped character
            Some(b'\\') if p_idx + 1 < pattern.len() => {
                if pattern[p_idx + 1] == string[s_idx] {
                    p_idx += 2;
                    s_idx += 1;
                } else {
                    // Mismatch, backtrack
                    if let (Some(p), Some(s)) = (star_p_idx, star_s_idx) {
                        p_idx = p;
                        s_idx = s + 1;
                        star_s_idx = Some(s + 1);
                    } else {
                        return false;
                    }
                }
            }
            // Exact character match
            Some(&p_char) if p_char == string[s_idx] => {
                p_idx += 1;
                s_idx += 1;
            }
            // Mismatch: backtrack to the last star if available
            _ => {
                if let (Some(p), Some(s)) = (star_p_idx, star_s_idx) {
                    p_idx = p;
                    s_idx = s + 1;
                    star_s_idx = Some(s + 1);
                } else {
                    return false;
                }
            }
        }
    }

    // After exhausting the string, consume any trailing stars in the pattern.
    while p_idx < pattern.len() && pattern[p_idx] == b'*' {
        p_idx += 1;
    }

    // Match is successful only if the entire pattern is consumed.
    p_idx == pattern.len()
}

/// Helper to parse a character set `[...]` and check if it matches a character.
/// Returns the length of the set pattern segment if it matches, otherwise `None`.
fn parse_char_set(pattern_segment: &[u8], char_to_match: u8) -> Option<usize> {
    if pattern_segment.len() < 3 || pattern_segment[0] != b'[' {
        return None;
    }

    let mut p_idx = 1;
    let negated = if pattern_segment.get(p_idx) == Some(&b'^') {
        p_idx += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    while p_idx < pattern_segment.len() && pattern_segment[p_idx] != b']' {
        let p_char = pattern_segment[p_idx];

        // Check for a range, e.g., `a-z`
        if p_idx + 2 < pattern_segment.len()
            && pattern_segment[p_idx + 1] == b'-'
            && pattern_segment[p_idx + 2] != b']'
        {
            let end_range = pattern_segment[p_idx + 2];
            if char_to_match >= p_char && char_to_match <= end_range {
                matched = true;
            }
            p_idx += 3;
        } else {
            // Single character match
            if p_char == char_to_match {
                matched = true;
            }
            p_idx += 1;
        }
    }

    if p_idx < pattern_segment.len() && (matched != negated) {
        Some(p_idx + 1) // Return total length including `[` and `]`
    } else {
        None
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
