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
) -> Result<(), SpinelDBError> {
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
    let domain_with_port = format!("{domain}:{port}"); // <-- FIXED AS PER CLIPPY SUGGESTION

    // `to_socket_addrs` can block, so we wrap it in `spawn_blocking`.
    let addrs = tokio::task::spawn_blocking(move || domain_with_port.to_socket_addrs())
        .await
        .map_err(|e| SpinelDBError::Internal(format!("DNS resolution task failed: {e}")))?
        .map_err(|e| {
            SpinelDBError::InvalidRequest(format!("Could not resolve domain '{domain}': {e}"))
        })?;

    let mut has_ips = false;
    for addr in addrs {
        has_ips = true;
        let ip = addr.ip();
        if !allow_private_ips && !is_globally_routable(&ip) {
            return Err(SpinelDBError::SecurityViolation(format!(
                "URL domain \"{domain}\" resolves to a forbidden IP address: {ip}"
            )));
        }
    }

    if !has_ips {
        return Err(SpinelDBError::InvalidRequest(format!(
            "Could not resolve domain '{domain}' to any IP addresses"
        )));
    }

    Ok(())
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
        if let Some(arg_str) = self.peek_str() {
            if arg_str.eq_ignore_ascii_case(flag_name) {
                self.cursor += 1;
                return true;
            }
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
        if let Some(arg_str) = self.peek_str() {
            if arg_str.eq_ignore_ascii_case(opt_name) {
                if self.cursor + 1 >= self.args.len() {
                    return Err(SpinelDBError::SyntaxError);
                }
                let value_str = extract_string(&self.args[self.cursor + 1])?;

                let parsed_value = value_str.parse::<T>().map_err(|e| {
                    SpinelDBError::InvalidState(format!(
                        "Invalid value for option '{opt_name}': {e}"
                    ))
                })?;

                self.cursor += 2; // Consume both the option name and its value
                return Ok(Some(parsed_value));
            }
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
    if remaining_args.is_empty() || remaining_args.len() % 2 != 0 {
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
