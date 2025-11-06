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
