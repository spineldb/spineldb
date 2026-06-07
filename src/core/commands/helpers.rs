// src/core/commands/helpers.rs

//! Provides helper functions for parsing command arguments from `RespFrame`s.
//! These helpers reduce boilerplate and ensure consistent error handling across commands.

use crate::core::SpinelDBError;
use crate::core::commands::zset::{ZaddCondition, ZaddUpdateRule};
use crate::core::protocol::RespFrame;
use bytes::Bytes;
use std::net::{IpAddr, ToSocketAddrs};
use std::str::FromStr;
use url::Url;
use wildmatch::WildMatch;

/// Validates a URL against a list of allowed domain glob patterns and checks for forbidden IP addresses.
///
/// This function provides SSRF (Server-Side Request Forgery) protection by resolving the
/// domain to its IP addresses and ensuring none of them fall within private, loopback,
/// or other non-globally-routable ranges, unless explicitly allowed by configuration.
///
/// # Arguments
/// * `url_str` - The URL to validate.
/// * `allowed_domains` - A slice of strings representing the glob patterns for allowed domains.
/// * `allow_private_ips` - A boolean flag to bypass the private IP check.
///
/// # Returns
/// `Ok(())` if the URL is valid and allowed.
/// `Err(SpinelDBError)` if the URL is invalid, its domain is not in the allowlist,
/// or it resolves to a forbidden IP address.
pub async fn validate_fetch_url(
    url_str: &str,
    allowed_domains: &[String],
    allow_private_ips: bool,
) -> Result<Vec<IpAddr>, SpinelDBError> {
    // Parse the URL to extract the domain.
    let url = Url::parse(url_str)
        .map_err(|_| SpinelDBError::InvalidRequest(format!("Invalid URL format: {url_str}")))?;

    let domain = url
        .host_str()
        .ok_or_else(|| SpinelDBError::InvalidRequest("URL must have a valid domain".to_string()))?;

    // If the allowlist is not empty, perform domain glob pattern validation.
    if !allowed_domains.is_empty() {
        let mut domain_allowed = false;
        for pattern in allowed_domains {
            if WildMatch::new(pattern).matches(domain) {
                domain_allowed = true;
                break;
            }
        }
        if !domain_allowed {
            return Err(SpinelDBError::SecurityViolation(format!(
                "URL domain \"{domain}\" is not in the list of allowed fetch domains."
            )));
        }
    }

    // --- SSRF Protection: Resolve domain to IP and validate ---
    // We need to include the port for `to_socket_addrs`. Use the URL's port or default.
    let port = url
        .port()
        .unwrap_or_else(|| if url.scheme() == "https" { 443 } else { 80 });
    let domain_with_port = format!("{domain}:{port}");

    // `to_socket_addrs` can block, so we wrap it in `spawn_blocking`.
    let addrs = tokio::task::spawn_blocking(move || domain_with_port.to_socket_addrs())
        .await
        .map_err(|e| SpinelDBError::Internal(format!("DNS resolution task failed: {e}")))?
        .map_err(|e| {
            SpinelDBError::InvalidRequest(format!("Could not resolve domain '{domain}': {e}"))
        })?;

    let mut resolved_ips = Vec::new();
    for addr in addrs {
        let ip = addr.ip();
        if !allow_private_ips && !is_globally_routable(&ip) {
            return Err(SpinelDBError::SecurityViolation(format!(
                "URL domain \"{domain}\" resolves to a forbidden IP address: {ip}"
            )));
        }
        resolved_ips.push(ip);
    }

    if resolved_ips.is_empty() {
        return Err(SpinelDBError::InvalidRequest(format!(
            "Could not resolve domain '{domain}' to any IP addresses"
        )));
    }

    Ok(resolved_ips)
}

/// Helper function to check if an IP address is globally routable.
/// This rejects private, loopback, and other special-use IP ranges.
fn is_globally_routable(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => {
            !ipv4.is_private()
                && !ipv4.is_loopback()
                && !ipv4.is_link_local()
                && !ipv4.is_broadcast()
                && !ipv4.is_documentation()
                && !ipv4.is_unspecified()
        }
        IpAddr::V6(ipv6) => {
            // A simple check for global scope is a good start.
            // `is_global` is experimental, so we replicate its logic.
            let is_global = (ipv6.segments()[0] & 0xe000) == 0x2000;
            !ipv6.is_loopback() && !ipv6.is_unspecified() && is_global
        }
    }
}

/// A type alias to simplify the complex return type of `parse_key_and_score_member_pairs`.
pub type ZaddParsedArgs = (
    Bytes,
    Vec<(f64, Bytes)>,
    ZaddCondition,
    ZaddUpdateRule,
    bool,
);

/// A helper struct to parse command arguments sequentially.
/// This simplifies parsing optional flags and value pairs.
pub struct ArgParser<'a> {
    args: &'a [RespFrame],
    cursor: usize,
}

impl<'a> ArgParser<'a> {
    /// Creates a new parser over a slice of arguments.
    pub fn new(args: &'a [RespFrame]) -> Self {
        Self { args, cursor: 0 }
    }

    /// Checks if the next argument matches a specific flag (case-insensitively).
    /// If it matches, consumes the argument and returns true.
    pub fn match_flag(&mut self, flag_name: &str) -> bool {
        if let Some(arg_str) = self.peek_str()
            && arg_str.eq_ignore_ascii_case(flag_name)
        {
            self.cursor += 1;
            return true;
        }
        false
    }

    /// Checks if the next argument matches an option name.
    /// If it matches, consumes both the option name and its value,
    /// then parses the value into the specified type `T`.
    pub fn match_option<T>(&mut self, opt_name: &str) -> Result<Option<T>, SpinelDBError>
    where
        T: FromStr,
        <T as FromStr>::Err: std::fmt::Display,
    {
        if let Some(arg_str) = self.peek_str()
            && arg_str.eq_ignore_ascii_case(opt_name)
        {
            if self.cursor + 1 >= self.args.len() {
                return Err(SpinelDBError::SyntaxError);
            }
            let value_str = extract_string(&self.args[self.cursor + 1])?;

            let parsed_value = value_str.parse::<T>().map_err(|e| {
                SpinelDBError::InvalidState(format!("Invalid value for option '{opt_name}': {e}"))
            })?;

            self.cursor += 2; // Consume both the option name and its value
            return Ok(Some(parsed_value));
        }
        Ok(None)
    }

    /// Returns the remaining arguments that have not been consumed.
    pub fn remaining_args(&self) -> &'a [RespFrame] {
        &self.args[self.cursor..]
    }

    /// Peeks at the next argument as a string without consuming it.
    fn peek_str(&self) -> Option<String> {
        self.args
            .get(self.cursor)
            .and_then(|frame| extract_string(frame).ok().map(|s| s.to_ascii_lowercase()))
    }
}

/// Extracts a `String` from a `RespFrame::BulkString`.
/// Returns a `WrongType` error if the frame is not a BulkString or not valid UTF-8.
pub fn extract_string(frame: &RespFrame) -> Result<String, SpinelDBError> {
    if let RespFrame::BulkString(bs) = frame {
        String::from_utf8(bs.to_vec()).map_err(|_| SpinelDBError::WrongType)
    } else {
        Err(SpinelDBError::WrongType)
    }
}

/// Extracts `Bytes` from a `RespFrame::BulkString`.
/// Returns a `WrongType` error if the frame is not a BulkString.
pub fn extract_bytes(frame: &RespFrame) -> Result<Bytes, SpinelDBError> {
    match frame {
        RespFrame::BulkString(bs) => Ok(bs.clone()),
        _ => Err(SpinelDBError::WrongType),
    }
}

/// Validates that the number of arguments matches an exact expected count.
pub fn validate_arg_count(
    args: &[RespFrame],
    expected: usize,
    cmd: &str,
) -> Result<(), SpinelDBError> {
    if args.len() != expected {
        Err(SpinelDBError::WrongArgumentCount(cmd.to_string()))
    } else {
        Ok(())
    }
}

/// Parses arguments for commands that follow the pattern `COMMAND key value1 [value2 ...]`.
pub fn parse_key_and_values(
    args: &[RespFrame],
    min_args: usize,
    cmd: &str,
) -> Result<(Bytes, Vec<Bytes>), SpinelDBError> {
    if args.len() < min_args {
        return Err(SpinelDBError::WrongArgumentCount(cmd.to_string()));
    }
    let key = extract_bytes(&args[0])?;
    let values = args[1..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<_, _>>()?;
    Ok((key, values))
}

/// Parses arguments for commands that follow the pattern `COMMAND key field1 value1 [field2 value2 ...]`.
pub fn parse_key_and_field_value_pairs(
    args: &[RespFrame],
    cmd: &str,
) -> Result<(Bytes, Vec<(Bytes, Bytes)>), SpinelDBError> {
    if args.len() < 3 || args.len() % 2 != 1 {
        return Err(SpinelDBError::WrongArgumentCount(cmd.to_string()));
    }
    let key = extract_bytes(&args[0])?;
    let fields = args[1..]
        .chunks_exact(2)
        .map(|chunk| -> Result<(Bytes, Bytes), SpinelDBError> {
            Ok((extract_bytes(&chunk[0])?, extract_bytes(&chunk[1])?))
        })
        .collect::<Result<_, _>>()?;
    Ok((key, fields))
}

/// Parses the complex arguments for the `ZADD` command, including optional flags
/// like `NX`, `XX`, `GT`, `LT`, and `CH`.
pub fn parse_key_and_score_member_pairs(
    args: &[RespFrame],
    cmd: &str,
) -> Result<ZaddParsedArgs, SpinelDBError> {
    if args.is_empty() {
        return Err(SpinelDBError::WrongArgumentCount(cmd.to_string()));
    }
    let key = extract_bytes(&args[0])?;

    // Use the new ArgParser to handle optional flags.
    let mut parser = ArgParser::new(&args[1..]);

    let condition = if parser.match_flag("nx") {
        ZaddCondition::IfNotExists
    } else if parser.match_flag("xx") {
        ZaddCondition::IfExists
    } else {
        ZaddCondition::None
    };

    let update_rule = if parser.match_flag("gt") {
        ZaddUpdateRule::GreaterThan
    } else if parser.match_flag("lt") {
        ZaddUpdateRule::LessThan
    } else {
        ZaddUpdateRule::None
    };

    let ch = parser.match_flag("ch");

    // Validate conflicting flags.
    if condition != ZaddCondition::None && update_rule != ZaddUpdateRule::None {
        return Err(SpinelDBError::SyntaxError);
    }

    let remaining_args = parser.remaining_args();

    // Ensure that the remaining arguments form valid score-member pairs.
    if remaining_args.is_empty() || !remaining_args.len().is_multiple_of(2) {
        return Err(SpinelDBError::WrongArgumentCount(cmd.to_string()));
    }

    let members = remaining_args
        .chunks_exact(2)
        .map(|chunk| -> Result<(f64, Bytes), SpinelDBError> {
            let score = extract_string(&chunk[0])?
                .parse::<f64>()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            let member = extract_bytes(&chunk[1])?;
            Ok((score, member))
        })
        .collect::<Result<_, _>>()?;

    Ok((key, members, condition, update_rule, ch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::commands::zset::{ZaddCondition, ZaddUpdateRule};

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_extract_string_from_bulk() {
        let frame = bs("hello");
        assert_eq!(extract_string(&frame).unwrap(), "hello");
    }

    #[test]
    fn test_extract_string_non_bulk_returns_wrong_type() {
        let frame = RespFrame::Integer(42);
        assert_eq!(extract_string(&frame), Err(SpinelDBError::WrongType));
    }

    #[test]
    fn test_extract_string_invalid_utf8_returns_wrong_type() {
        let frame = RespFrame::BulkString(Bytes::from_static(&[0xFF, 0xFE]));
        assert_eq!(extract_string(&frame), Err(SpinelDBError::WrongType));
    }

    #[test]
    fn test_extract_bytes_from_bulk() {
        let frame = bs("hello");
        assert_eq!(extract_bytes(&frame).unwrap(), Bytes::from_static(b"hello"));
    }

    #[test]
    fn test_extract_bytes_non_bulk_returns_wrong_type() {
        let frame = RespFrame::SimpleString("x".to_string());
        assert_eq!(extract_bytes(&frame), Err(SpinelDBError::WrongType));
    }

    #[test]
    fn test_validate_arg_count_ok() {
        let args = vec![bs("a"), bs("b")];
        validate_arg_count(&args, 2, "TEST").unwrap();
    }

    #[test]
    fn test_validate_arg_count_mismatch() {
        let args = vec![bs("a")];
        let r = validate_arg_count(&args, 2, "TEST");
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_parse_key_and_values_ok() {
        let args = vec![bs("mykey"), bs("v1"), bs("v2")];
        let (key, vals) = parse_key_and_values(&args, 2, "RPUSH").unwrap();
        assert_eq!(key, Bytes::from_static(b"mykey"));
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], Bytes::from_static(b"v1"));
    }

    #[test]
    fn test_parse_key_and_values_too_few() {
        let args = vec![bs("k")];
        let r = parse_key_and_values(&args, 2, "RPUSH");
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_parse_key_and_field_value_pairs_ok() {
        let args = vec![bs("mykey"), bs("f1"), bs("v1"), bs("f2"), bs("v2")];
        let (key, pairs) = parse_key_and_field_value_pairs(&args, "HSET").unwrap();
        assert_eq!(key, Bytes::from_static(b"mykey"));
        assert_eq!(pairs.len(), 2);
        assert_eq!(
            pairs[0],
            (Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))
        );
    }

    #[test]
    fn test_parse_key_and_field_value_pairs_odd_count() {
        let args = vec![bs("mykey"), bs("f1"), bs("v1"), bs("f2")];
        let r = parse_key_and_field_value_pairs(&args, "HSET");
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_argparser_match_flag_case_insensitive() {
        let args = vec![bs("NX"), bs("10"), bs("member")];
        let mut p = ArgParser::new(&args);
        assert!(p.match_flag("nx"));
        // Cursor advanced past the flag.
        assert_eq!(p.remaining_args().len(), 2);
    }

    #[test]
    fn test_argparser_match_flag_miss() {
        let args = vec![bs("10")];
        let mut p = ArgParser::new(&args);
        assert!(!p.match_flag("nx"));
        // Cursor unchanged.
        assert_eq!(p.remaining_args().len(), 1);
    }

    #[test]
    fn test_argparser_match_option_ok() {
        let args = vec![bs("LIMIT"), bs("0"), bs("10")];
        let mut p = ArgParser::new(&args);
        let v: Option<u64> = p.match_option("limit").unwrap();
        assert_eq!(v, Some(0));
        // Cursor advanced past both the option name and its value.
        assert_eq!(p.remaining_args().len(), 1);
    }

    #[test]
    fn test_argparser_match_option_missing_value() {
        let args = vec![bs("LIMIT")];
        let mut p = ArgParser::new(&args);
        let r: Result<Option<u64>, _> = p.match_option("LIMIT");
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_argparser_match_option_parse_error() {
        let args = vec![bs("LIMIT"), bs("notanumber")];
        let mut p = ArgParser::new(&args);
        let r: Result<Option<u64>, _> = p.match_option("LIMIT");
        assert!(matches!(r, Err(SpinelDBError::InvalidState(_))));
    }

    #[test]
    fn test_argparser_match_option_not_present() {
        let args = vec![bs("OTHER")];
        let mut p = ArgParser::new(&args);
        let v: Option<u64> = p.match_option("LIMIT").unwrap();
        assert!(v.is_none());
    }

    #[test]
    fn test_argparser_remaining_args_on_empty() {
        let p = ArgParser::new(&[]);
        assert!(p.remaining_args().is_empty());
    }

    #[test]
    fn test_parse_zadd_args_basic() {
        let args = vec![bs("mykey"), bs("1.5"), bs("alpha")];
        let (key, members, cond, rule, ch) =
            parse_key_and_score_member_pairs(&args, "ZADD").unwrap();
        assert_eq!(key, Bytes::from_static(b"mykey"));
        assert_eq!(members, vec![(1.5, Bytes::from_static(b"alpha"))]);
        assert_eq!(cond, ZaddCondition::None);
        assert_eq!(rule, ZaddUpdateRule::None);
        assert!(!ch);
    }

    #[test]
    fn test_parse_zadd_args_with_nx() {
        let args = vec![bs("k"), bs("NX"), bs("1"), bs("a"), bs("2"), bs("b")];
        let (_, _, cond, _, _) = parse_key_and_score_member_pairs(&args, "ZADD").unwrap();
        assert_eq!(cond, ZaddCondition::IfNotExists);
    }

    #[test]
    fn test_parse_zadd_args_with_xx_and_gt_conflict() {
        // XX and GT are mutually exclusive.
        let args = vec![bs("k"), bs("XX"), bs("GT"), bs("1"), bs("a")];
        assert!(matches!(
            parse_key_and_score_member_pairs(&args, "ZADD"),
            Err(SpinelDBError::SyntaxError)
        ));
    }

    #[test]
    fn test_parse_zadd_args_with_ch() {
        let args = vec![bs("k"), bs("CH"), bs("1"), bs("a")];
        let (_, _, _, _, ch) = parse_key_and_score_member_pairs(&args, "ZADD").unwrap();
        assert!(ch);
    }

    #[test]
    fn test_parse_zadd_args_odd_pairs() {
        // Pairs must come in (score, member) tuples.
        let args = vec![bs("k"), bs("1")];
        assert!(matches!(
            parse_key_and_score_member_pairs(&args, "ZADD"),
            Err(SpinelDBError::WrongArgumentCount(_))
        ));
    }

    #[test]
    fn test_parse_zadd_args_no_members() {
        let args = vec![bs("k")];
        assert!(matches!(
            parse_key_and_score_member_pairs(&args, "ZADD"),
            Err(SpinelDBError::WrongArgumentCount(_))
        ));
    }

    #[test]
    fn test_parse_zadd_args_invalid_score() {
        let args = vec![bs("k"), bs("not-a-number"), bs("a")];
        let r = parse_key_and_score_member_pairs(&args, "ZADD");
        assert!(matches!(r, Err(SpinelDBError::NotAFloat)));
    }
}
