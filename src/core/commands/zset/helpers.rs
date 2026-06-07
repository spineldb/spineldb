// src/core/commands/zset/helpers.rs

use crate::core::commands::helpers::extract_string;
use crate::core::database::zset::{LexBoundary, ScoreBoundary, ZSetEntry};
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use bytes::Bytes;

// Helper untuk mem-parsing batas skor seperti "10", "(10", "-inf"
pub(super) fn parse_score_boundary(s: &str) -> Result<ScoreBoundary, SpinelDBError> {
    match s.to_ascii_lowercase().as_str() {
        "-inf" => Ok(ScoreBoundary::NegInfinity),
        "+inf" => Ok(ScoreBoundary::PosInfinity),
        s => {
            if let Some(num_str) = s.strip_prefix('(') {
                let score = num_str.parse().map_err(|_| SpinelDBError::NotAFloat)?;
                Ok(ScoreBoundary::Exclusive(score))
            } else {
                let score = s.parse().map_err(|_| SpinelDBError::NotAFloat)?;
                Ok(ScoreBoundary::Inclusive(score))
            }
        }
    }
}

// [NEW] Helper untuk mem-parsing batas leksikografis seperti "[lex" atau "(lex"
pub(super) fn parse_lex_boundary(s: &str) -> Result<LexBoundary, SpinelDBError> {
    match s {
        "-" => Ok(LexBoundary::Min),
        "+" => Ok(LexBoundary::Max),
        s => {
            if let Some(val) = s.strip_prefix('[') {
                if !val.ends_with(']') {
                    // Ini sebenarnya tidak diperlukan karena SpinelDB akan menangani
                    // string `[abc` sebagai `[abc]`. Kita akan menirunya.
                    // Namun, untuk validasi yang lebih ketat, bisa diaktifkan.
                    // return Err(SpinelDBError::SyntaxError);
                }
                let inner = val.strip_suffix(']').unwrap_or(val);
                Ok(LexBoundary::Inclusive(Bytes::from(inner.to_string())))
            } else if let Some(val) = s.strip_prefix('(') {
                if !val.ends_with(')') {
                    // Sama seperti di atas
                }
                let inner = val.strip_suffix(')').unwrap_or(val);
                Ok(LexBoundary::Exclusive(Bytes::from(inner.to_string())))
            } else {
                // SpinelDB akan menganggap "abc" sebagai syntax error di konteks ini.
                Err(SpinelDBError::SyntaxError)
            }
        }
    }
}

// Helper untuk memformat hasil ZRANGE menjadi RespValue
pub(super) fn format_zrange_response(range: Vec<ZSetEntry>, with_scores: bool) -> RespValue {
    if range.is_empty() {
        return RespValue::Array(vec![]);
    }
    let mut response = Vec::with_capacity(range.len() * if with_scores { 2 } else { 1 });
    for entry in range {
        response.push(RespValue::BulkString(entry.member));
        if with_scores {
            response.push(RespValue::BulkString(entry.score.to_string().into()));
        }
    }
    RespValue::Array(response)
}

// Helper untuk mem-parsing argumen ZRANGE/ZREVRANGE
pub(super) fn parse_range_args(args: &[RespFrame]) -> Result<(i64, i64, bool), SpinelDBError> {
    let start = extract_string(&args[1])?
        .parse()
        .map_err(|_| SpinelDBError::NotAnInteger)?;
    let stop = extract_string(&args[2])?
        .parse()
        .map_err(|_| SpinelDBError::NotAnInteger)?;
    let mut with_scores = false;
    if args.len() == 4 {
        if extract_string(&args[3])?.eq_ignore_ascii_case("withscores") {
            with_scores = true;
        } else {
            return Err(SpinelDBError::SyntaxError);
        }
    }
    Ok((start, stop, with_scores))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::database::zset::{LexBoundary, ScoreBoundary, ZSetEntry};

    fn bs(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn bs_frame(s: &str) -> RespFrame {
        RespFrame::BulkString(bs(s))
    }

    #[test]
    fn test_parse_score_boundary_neg_inf() {
        assert!(matches!(
            parse_score_boundary("-inf").unwrap(),
            ScoreBoundary::NegInfinity
        ));
    }

    #[test]
    fn test_parse_score_boundary_pos_inf() {
        assert!(matches!(
            parse_score_boundary("+inf").unwrap(),
            ScoreBoundary::PosInfinity
        ));
    }

    #[test]
    fn test_parse_score_boundary_inclusive() {
        let r = parse_score_boundary("10").unwrap();
        assert!(matches!(r, ScoreBoundary::Inclusive(v) if (v - 10.0).abs() < 1e-9));
    }

    #[test]
    fn test_parse_score_boundary_exclusive() {
        let r = parse_score_boundary("(10").unwrap();
        assert!(matches!(r, ScoreBoundary::Exclusive(v) if (v - 10.0).abs() < 1e-9));
    }

    #[test]
    fn test_parse_score_boundary_invalid_is_error() {
        assert!(matches!(
            parse_score_boundary("abc").unwrap_err(),
            SpinelDBError::NotAFloat
        ));
    }

    #[test]
    fn test_parse_score_boundary_case_insensitive() {
        assert!(matches!(
            parse_score_boundary("-INF").unwrap(),
            ScoreBoundary::NegInfinity
        ));
    }

    #[test]
    fn test_parse_lex_boundary_min_max() {
        assert!(matches!(parse_lex_boundary("-").unwrap(), LexBoundary::Min));
        assert!(matches!(parse_lex_boundary("+").unwrap(), LexBoundary::Max));
    }

    #[test]
    fn test_parse_lex_boundary_inclusive() {
        let r = parse_lex_boundary("[abc]").unwrap();
        if let LexBoundary::Inclusive(b) = r {
            assert_eq!(b, bs("abc"));
        } else {
            panic!("expected Inclusive");
        }
    }

    #[test]
    fn test_parse_lex_boundary_exclusive() {
        let r = parse_lex_boundary("(abc)").unwrap();
        if let LexBoundary::Exclusive(b) = r {
            assert_eq!(b, bs("abc"));
        } else {
            panic!("expected Exclusive");
        }
    }

    #[test]
    fn test_parse_lex_boundary_unprefixed_is_syntax_error() {
        assert!(matches!(
            parse_lex_boundary("abc").unwrap_err(),
            SpinelDBError::SyntaxError
        ));
    }

    #[test]
    fn test_format_zrange_response_empty() {
        let r = format_zrange_response(vec![], false);
        if let RespValue::Array(arr) = r {
            assert!(arr.is_empty());
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_format_zrange_response_without_scores() {
        let entries = vec![
            ZSetEntry { score: 1.0, member: bs("a") },
            ZSetEntry { score: 2.0, member: bs("b") },
        ];
        let r = format_zrange_response(entries, false);
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 2);
            assert!(matches!(&arr[0], RespValue::BulkString(b) if b == &bs("a")));
            assert!(matches!(&arr[1], RespValue::BulkString(b) if b == &bs("b")));
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_format_zrange_response_with_scores() {
        let entries = vec![ZSetEntry { score: 1.5, member: bs("a") }];
        let r = format_zrange_response(entries, true);
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 2);
            assert!(matches!(&arr[0], RespValue::BulkString(b) if b == &bs("a")));
            if let RespValue::BulkString(b) = &arr[1] {
                let s = std::str::from_utf8(b).unwrap();
                assert_eq!(s, "1.5");
            } else {
                panic!("expected BulkString score");
            }
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_parse_range_args_minimal() {
        let args = [bs_frame("ZRANGE"), bs_frame("0"), bs_frame("-1")];
        let (s, e, ws) = parse_range_args(&args).unwrap();
        assert_eq!(s, 0);
        assert_eq!(e, -1);
        assert!(!ws);
    }

    #[test]
    fn test_parse_range_args_with_withscores() {
        let args = [bs_frame("ZRANGE"), bs_frame("0"), bs_frame("10"), bs_frame("WITHSCORES")];
        let (s, e, ws) = parse_range_args(&args).unwrap();
        assert_eq!(s, 0);
        assert_eq!(e, 10);
        assert!(ws);
    }

    #[test]
    fn test_parse_range_args_withscores_case_insensitive() {
        let args = [bs_frame("ZRANGE"), bs_frame("0"), bs_frame("-1"), bs_frame("withscores")];
        let (_, _, ws) = parse_range_args(&args).unwrap();
        assert!(ws);
    }

    #[test]
    fn test_parse_range_args_invalid_option_is_syntax_error() {
        let args = [bs_frame("ZRANGE"), bs_frame("0"), bs_frame("10"), bs_frame("LIMIT")];
        let r = parse_range_args(&args);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_parse_range_args_invalid_start_is_not_integer() {
        let args = [bs_frame("ZRANGE"), bs_frame("abc"), bs_frame("0")];
        let r = parse_range_args(&args);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }

    #[test]
    fn test_parse_range_args_invalid_stop_is_not_integer() {
        let args = [bs_frame("ZRANGE"), bs_frame("0"), bs_frame("xyz")];
        let r = parse_range_args(&args);
        assert!(matches!(r, Err(SpinelDBError::NotAnInteger)));
    }
}
