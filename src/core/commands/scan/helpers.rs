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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::protocol::RespFrame;

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match(b"foo", b"foo"));
        assert!(!glob_match(b"foo", b"bar"));
        assert!(!glob_match(b"foo", b"fooo"));
        assert!(!glob_match(b"fooo", b"foo"));
    }

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"foo"));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"foo*", b"foo"));
        assert!(glob_match(b"foo*", b"foobar"));
        assert!(!glob_match(b"foo*", b"barfoo"));
        assert!(glob_match(b"a*b", b"ab"));
        assert!(glob_match(b"a*b", b"aXXXb"));
        assert!(!glob_match(b"a*b", b"axbX"));
    }

    #[test]
    fn test_glob_match_question_matches_one_byte_only() {
        // `?` matches exactly one byte (not one Unicode scalar).
        assert!(glob_match(b"?", b"a"));
        // 2-byte input: glob is consumed but string still has unmatched bytes.
        assert!(!glob_match(b"?", b"\xC3\xA9"));
        // 2-byte pattern to match a 2-byte string.
        assert!(glob_match(b"??", b"\xC3\xA9"));
    }

    #[test]
    fn test_glob_match_char_set() {
        assert!(glob_match(b"[abc]", b"a"));
        assert!(glob_match(b"[abc]", b"b"));
        assert!(glob_match(b"[abc]", b"c"));
        assert!(!glob_match(b"[abc]", b"d"));
    }

    #[test]
    fn test_glob_match_negated_char_set() {
        assert!(!glob_match(b"[^abc]", b"a"));
        assert!(glob_match(b"[^abc]", b"d"));
    }

    #[test]
    fn test_glob_match_char_range() {
        assert!(glob_match(b"[a-z]", b"m"));
        assert!(!glob_match(b"[a-z]", b"A"));
        assert!(glob_match(b"[0-9]", b"5"));
    }

    #[test]
    fn test_glob_match_escape() {
        // A backslash escapes the next character so the * is treated literally.
        assert!(glob_match(br"a\*b", b"a*b"));
        assert!(!glob_match(br"a\*b", b"axb"));
    }

    #[test]
    fn test_glob_match_complex_pattern() {
        assert!(glob_match(b"user:*:profile", b"user:100:profile"));
        assert!(glob_match(b"user:*:profile", b"user:100:profile"));
        assert!(!glob_match(b"user:*:profile", b"user:100:settings"));
    }

    #[test]
    fn test_glob_match_multiple_stars() {
        assert!(glob_match(b"*foo*", b"xfooy"));
        assert!(glob_match(b"*foo*", b"foo"));
        assert!(glob_match(b"*foo*", b"xfoo"));
        assert!(!glob_match(b"*foo*", b"xoxox"));
    }

    #[test]
    fn test_glob_match_empty_pattern_matches_empty_string() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"a"));
    }

    #[test]
    fn test_glob_match_handles_backtracking() {
        // This is a classic backtracking case where the first '*' is greedy
        // but must back off to find a match.
        assert!(glob_match(b"a*a*a", b"aaa"));
        assert!(glob_match(b"a*ab", b"aXab"));
        assert!(glob_match(b"*ab*", b"ab"));
    }

    #[test]
    fn test_scan_cursor_roundtrip() {
        for shard in [0usize, 1, 16, 128, 255] {
            for internal in [0usize, 1, 100, 0x00FFFF] {
                let cursor = encode_scan_cursor(shard, internal);
                let (s, i) = decode_scan_cursor(cursor);
                assert_eq!(s, shard);
                assert_eq!(i, internal);
            }
        }
    }

    #[test]
    fn test_scan_cursor_isolation() {
        // A 1 in shard 0, internal 1 must NOT equal shard 1, internal 0.
        let a = encode_scan_cursor(0, 1);
        let b = encode_scan_cursor(1, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn test_scan_cursor_max_shard() {
        // Top 8 bits hold the shard, so a value with the top byte set encodes shard 255.
        let cursor = encode_scan_cursor(255, 0);
        let (s, _) = decode_scan_cursor(cursor);
        assert_eq!(s, 255);
    }

    #[test]
    fn test_parse_scan_args_minimal() {
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"0"))];
        let (cursor, pattern, count) = parse_scan_args(&args, 1, "SCAN").unwrap();
        assert_eq!(cursor, 0);
        assert!(pattern.is_none());
        assert!(count.is_none());
    }

    #[test]
    fn test_parse_scan_args_full() {
        let args = vec![
            RespFrame::BulkString(Bytes::from_static(b"42")),
            RespFrame::BulkString(Bytes::from_static(b"MATCH")),
            RespFrame::BulkString(Bytes::from_static(b"user:*")),
            RespFrame::BulkString(Bytes::from_static(b"COUNT")),
            RespFrame::BulkString(Bytes::from_static(b"50")),
        ];
        let (cursor, pattern, count) = parse_scan_args(&args, 1, "SCAN").unwrap();
        assert_eq!(cursor, 42);
        assert_eq!(pattern, Some(Bytes::from_static(b"user:*")));
        assert_eq!(count, Some(50));
    }

    #[test]
    fn test_parse_scan_args_case_insensitive() {
        let args = vec![
            RespFrame::BulkString(Bytes::from_static(b"0")),
            RespFrame::BulkString(Bytes::from_static(b"match")),
            RespFrame::BulkString(Bytes::from_static(b"k*")),
        ];
        let (_, pattern, _) = parse_scan_args(&args, 1, "SCAN").unwrap();
        assert!(pattern.is_some());
    }

    #[test]
    fn test_parse_scan_args_match_without_value_is_error() {
        let args = vec![
            RespFrame::BulkString(Bytes::from_static(b"0")),
            RespFrame::BulkString(Bytes::from_static(b"MATCH")),
        ];
        assert!(parse_scan_args(&args, 1, "SCAN").is_err());
    }

    #[test]
    fn test_parse_scan_args_count_without_value_is_error() {
        let args = vec![
            RespFrame::BulkString(Bytes::from_static(b"0")),
            RespFrame::BulkString(Bytes::from_static(b"COUNT")),
        ];
        assert!(parse_scan_args(&args, 1, "SCAN").is_err());
    }

    #[test]
    fn test_parse_scan_args_unknown_option_is_error() {
        let args = vec![
            RespFrame::BulkString(Bytes::from_static(b"0")),
            RespFrame::BulkString(Bytes::from_static(b"FOO")),
        ];
        assert!(parse_scan_args(&args, 1, "SCAN").is_err());
    }

    #[test]
    fn test_parse_scan_args_non_numeric_cursor_is_error() {
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"abc"))];
        assert!(parse_scan_args(&args, 1, "SCAN").is_err());
    }

    #[test]
    fn test_parse_scan_args_too_few_args_is_error() {
        let args: Vec<RespFrame> = vec![];
        assert!(parse_scan_args(&args, 1, "SCAN").is_err());
    }

    #[test]
    fn test_format_scan_options_empty() {
        let bytes = format_scan_options_to_bytes(&None, &None);
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_format_scan_options_pattern_only() {
        let bytes = format_scan_options_to_bytes(&Some(Bytes::from_static(b"k:*")), &None);
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[0], Bytes::from_static(b"MATCH"));
        assert_eq!(bytes[1], Bytes::from_static(b"k:*"));
    }

    #[test]
    fn test_format_scan_options_count_only() {
        let bytes = format_scan_options_to_bytes(&None, &Some(10));
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[0], Bytes::from_static(b"COUNT"));
        assert_eq!(bytes[1], Bytes::from_static(b"10"));
    }

    #[test]
    fn test_format_scan_options_both() {
        let bytes = format_scan_options_to_bytes(&Some(Bytes::from_static(b"x*")), &Some(20));
        assert_eq!(bytes.len(), 4);
    }
}
