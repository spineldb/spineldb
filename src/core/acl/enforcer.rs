// src/core/acl/enforcer.rs

use crate::config::AclConfig;
use crate::core::acl::parsed_rules::{
    AclCommandRule, AclKeyRule, AclPubSubRule, ParsedAclCondition, ParsedAclRule,
};
use crate::core::acl::user::AclUser;
use crate::core::commands::command_trait::CommandFlags;
use crate::core::protocol::RespFrame;
use regex::Regex;
use std::collections::HashMap;
use tracing::warn;

/// Enforces Access Control List (ACL) rules.
#[derive(Debug)]
pub struct AclEnforcer {
    enabled: bool,
    /// Holds the rules parsed from the config into a more efficient internal format.
    rules: HashMap<String, ParsedAclRule>,
}

impl AclEnforcer {
    /// Creates a new AclEnforcer, parsing user-facing rules from the config
    /// into an optimized internal representation.
    pub fn new(config: &AclConfig) -> Self {
        let mut rules_map = HashMap::new();

        for rule in &config.rules {
            let mut parsed_rule = ParsedAclRule {
                name: rule.name.clone(),
                ..Default::default()
            };

            // Parse command rules (e.g., "+get", "-@write").
            if let Some(commands) = &rule.commands {
                for cmd_rule in commands {
                    parsed_rule
                        .commands
                        .push(Self::parse_command_rule(cmd_rule));
                }
            }

            // Parse key pattern rules (e.g., "~key:*").
            if let Some(keys) = &rule.keys {
                for key_pattern in keys {
                    if let Some(rule) = Self::parse_pattern_rule(key_pattern, "~", "-") {
                        parsed_rule.keys.push(rule);
                    } else if key_pattern.eq_ignore_ascii_case("allkeys") {
                        parsed_rule.keys.push(AclKeyRule::All);
                    }
                }
            }

            // Parse Pub/Sub channel pattern rules (e.g., "&news:*").
            if let Some(channel_patterns) = &rule.pubsub_channels {
                for channel_pattern in channel_patterns {
                    if let Some(rule) = Self::parse_pattern_rule(channel_pattern, "&", "-") {
                        parsed_rule.pubsub_channels.push(rule);
                    } else if channel_pattern.eq_ignore_ascii_case("allchannels") {
                        parsed_rule.pubsub_channels.push(AclPubSubRule::All);
                    }
                }
            }

            // Parse dynamic condition rules.
            for condition in &rule.conditions {
                let parsed_condition = ParsedAclCondition {
                    target: condition.target.clone(),
                    operator: condition.operator.clone(),
                    rules_on_match: condition
                        .result
                        .iter()
                        .map(|s| Self::parse_command_rule(s))
                        .collect(),
                };
                parsed_rule.conditions.push(parsed_condition);
            }

            rules_map.insert(rule.name.clone(), parsed_rule);
        }

        AclEnforcer {
            enabled: config.enabled,
            rules: rules_map,
        }
    }

    /// The main permission checking function.
    pub fn check_permission(
        &self,
        user: Option<&AclUser>,
        raw_args: &[RespFrame],
        command_name: &str,
        command_flags: CommandFlags,
        keys: &[String],
        pubsub_channels: &[String],
    ) -> bool {
        if !self.enabled {
            return true;
        }

        // The AUTH command is a special case that must be allowed before authentication.
        if user.is_none() && command_name.eq_ignore_ascii_case("AUTH") {
            return true;
        }

        let Some(user) = user else {
            return false;
        };

        let user_rules: Vec<&ParsedAclRule> = user
            .rules
            .iter()
            .filter_map(|rule_name| self.rules.get(rule_name))
            .collect();

        // 1. Check static command and category rules first.
        let mut final_verdict =
            self.check_static_command_permission(&user_rules, command_name, command_flags);

        // 2. Modify verdict based on dynamic conditions.
        final_verdict = self.check_condition_permission(
            &user_rules,
            raw_args,
            keys,
            command_name,
            command_flags,
            final_verdict,
        );

        if !final_verdict {
            return false;
        }

        // 3. Check key and pub/sub permissions if command permission was granted.
        if !self.check_key_permission(&user_rules, keys) {
            return false;
        }
        if !self.check_pubsub_permission(&user_rules, pubsub_channels) {
            return false;
        }

        true
    }

    /// Parses a single command rule string into its internal enum representation.
    fn parse_command_rule(rule_str: &str) -> AclCommandRule {
        if let Some(cat_str) = rule_str.strip_prefix("+@") {
            if cat_str.eq_ignore_ascii_case("all") {
                AclCommandRule::All
            } else {
                AclCommandRule::AllowCategory(Self::category_str_to_flags(cat_str))
            }
        } else if let Some(cat_str) = rule_str.strip_prefix("-@") {
            AclCommandRule::DenyCategory(Self::category_str_to_flags(cat_str))
        } else if let Some(cmd) = rule_str.strip_prefix('+') {
            AclCommandRule::Allow(cmd.to_string())
        } else if let Some(cmd) = rule_str.strip_prefix('-') {
            AclCommandRule::Deny(cmd.to_string())
        } else {
            // Default to allow if no prefix is present (SpinelDB behavior).
            AclCommandRule::Allow(rule_str.to_string())
        }
    }

    /// Parses a key or pub/sub glob-style pattern into a Regex-based rule.
    fn parse_pattern_rule<T>(pattern_str: &str, allow_prefix: &str, deny_prefix: &str) -> Option<T>
    where
        T: From<(Regex, bool)>, // (Regex, is_allow_rule)
    {
        let (pattern, is_allow) = if let Some(p) = pattern_str.strip_prefix(allow_prefix) {
            (p, true)
        } else if let Some(p) = pattern_str.strip_prefix(deny_prefix) {
            (p, false)
        } else {
            return None;
        };

        // Convert SpinelDB glob-style pattern to a valid regex.
        let mut regex_pattern = String::with_capacity(pattern.len() * 2);
        regex_pattern.push('^');
        let mut chars = pattern.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '*' => regex_pattern.push_str(".*"),
                '?' => regex_pattern.push('.'),
                '[' => {
                    regex_pattern.push('[');
                    if chars.peek() == Some(&'^') {
                        regex_pattern.push('^');
                        chars.next();
                    }
                    for pc in chars.by_ref() {
                        if pc == ']' {
                            break;
                        }
                        regex_pattern.push(pc);
                    }
                    regex_pattern.push(']');
                }
                '\\' => {
                    if let Some(next_char) = chars.next() {
                        regex_pattern.push_str(&regex::escape(&next_char.to_string()));
                    }
                }
                _ => regex_pattern.push_str(&regex::escape(&c.to_string())),
            }
        }
        regex_pattern.push('$');

        match Regex::new(&regex_pattern) {
            Ok(regex) => Some(T::from((regex, is_allow))),
            Err(e) => {
                warn!(r#"Invalid ACL regex pattern "{}": {}"#, pattern_str, e);
                None
            }
        }
    }

    /// Checks if a command is allowed based on the user's static rules.
    fn check_static_command_permission(
        &self,
        rules: &[&ParsedAclRule],
        cmd_name: &str,
        cmd_flags: CommandFlags,
    ) -> bool {
        let mut final_verdict = false; // Default-deny.
        for rule in rules {
            for cmd_rule in &rule.commands {
                match cmd_rule {
                    AclCommandRule::All => final_verdict = true,
                    AclCommandRule::Allow(cmd) if cmd.eq_ignore_ascii_case(cmd_name) => {
                        final_verdict = true
                    }
                    AclCommandRule::AllowCategory(flags) if cmd_flags.contains(*flags) => {
                        final_verdict = true
                    }
                    // A deny rule is an immediate rejection.
                    AclCommandRule::Deny(cmd) if cmd.eq_ignore_ascii_case(cmd_name) => {
                        return false;
                    }
                    AclCommandRule::DenyCategory(flags) if cmd_flags.contains(*flags) => {
                        return false;
                    }
                    _ => {}
                }
            }
        }
        final_verdict
    }

    /// Evaluates a single ACL condition.
    fn evaluate_condition(
        &self,
        condition: &ParsedAclCondition,
        raw_args: &[RespFrame],
        keys: &[String],
    ) -> bool {
        use crate::core::acl::rules::ConditionTarget;

        match &condition.target {
            ConditionTarget::Key { index } => {
                let key_subject = keys.get(*index).map(AsRef::as_ref).unwrap_or("");
                self.evaluate_operator(key_subject, &condition.operator, raw_args)
            }
            ConditionTarget::Arg { index } => {
                let arg_subject = raw_args.get(*index).and_then(|frame| {
                    if let RespFrame::BulkString(bs) = frame {
                        Some(String::from_utf8_lossy(bs))
                    } else {
                        None
                    }
                });
                self.evaluate_operator(
                    arg_subject.as_deref().unwrap_or(""),
                    &condition.operator,
                    raw_args,
                )
            }
            ConditionTarget::Command => self.evaluate_operator("", &condition.operator, raw_args),
        }
    }

    /// Evaluates dynamic conditions and modifies the permission verdict.
    fn check_condition_permission(
        &self,
        rules: &[&ParsedAclRule],
        raw_args: &[RespFrame],
        keys: &[String],
        cmd_name: &str,
        cmd_flags: CommandFlags,
        mut current_verdict: bool,
    ) -> bool {
        for rule in rules {
            for condition in &rule.conditions {
                if self.evaluate_condition(condition, raw_args, keys) {
                    // If the condition is met, apply its result rules.
                    for action in &condition.rules_on_match {
                        match action {
                            AclCommandRule::All
                            | AclCommandRule::Allow(_)
                            | AclCommandRule::AllowCategory(_) => {
                                current_verdict = true;
                            }
                            AclCommandRule::Deny(cmd) if cmd.eq_ignore_ascii_case(cmd_name) => {
                                return false; // Immediate deny
                            }
                            AclCommandRule::DenyCategory(flags) if cmd_flags.contains(*flags) => {
                                return false; // Immediate deny
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        current_verdict
    }

    /// Helper to evaluate a specific operator against a subject string.
    fn evaluate_operator(
        &self,
        subject: &str,
        operator: &crate::core::acl::rules::ConditionOperator,
        raw_args: &[RespFrame],
    ) -> bool {
        use crate::core::acl::rules::ConditionOperator;
        match operator {
            ConditionOperator::StartsWith(prefix) => subject.starts_with(prefix),
            ConditionOperator::Equals(value) => subject == value,
            ConditionOperator::IsNumber => subject.parse::<f64>().is_ok(),
            ConditionOperator::ArgcLessThan(val) => (raw_args.len() + 1) < *val,
            ConditionOperator::ArgcGreaterThan(val) => (raw_args.len() + 1) > *val,
        }
    }

    /// Checks if access to a set of keys is allowed.
    fn check_key_permission(&self, rules: &[&ParsedAclRule], keys: &[String]) -> bool {
        if keys.is_empty() {
            return true;
        }
        let all_key_rules: Vec<_> = rules.iter().flat_map(|r| &r.keys).collect();
        if all_key_rules.iter().any(|r| matches!(r, AclKeyRule::All)) {
            return true;
        }

        for key in keys {
            let mut allowed = false;
            for rule in &all_key_rules {
                match rule {
                    AclKeyRule::Allow(regex) if regex.is_match(key) => allowed = true,
                    AclKeyRule::Deny(regex) if regex.is_match(key) => return false,
                    _ => {}
                }
            }
            if !allowed {
                return false;
            }
        }
        true
    }

    /// Checks if access to a set of Pub/Sub channels is allowed.
    fn check_pubsub_permission(&self, rules: &[&ParsedAclRule], channels: &[String]) -> bool {
        if channels.is_empty() {
            return true;
        }
        let all_pubsub_rules: Vec<_> = rules.iter().flat_map(|r| &r.pubsub_channels).collect();
        if all_pubsub_rules
            .iter()
            .any(|r| matches!(r, AclPubSubRule::All))
        {
            return true;
        }

        for channel in channels {
            let mut allowed = false;
            for rule in &all_pubsub_rules {
                match rule {
                    AclPubSubRule::Allow(regex) if regex.is_match(channel) => allowed = true,
                    AclPubSubRule::Deny(regex) if regex.is_match(channel) => return false,
                    _ => {}
                }
            }
            if !allowed {
                return false;
            }
        }
        true
    }

    /// Converts a category string (e.g., "write") into its corresponding CommandFlags.
    fn category_str_to_flags(cat: &str) -> CommandFlags {
        match cat {
            "write" => CommandFlags::WRITE,
            "read" => CommandFlags::READONLY,
            "admin" => CommandFlags::ADMIN,
            "pubsub" => CommandFlags::PUBSUB,
            "transaction" => CommandFlags::TRANSACTION,
            "dangerous" => CommandFlags::empty(),
            "connection" => CommandFlags::empty(),
            _ => CommandFlags::empty(),
        }
    }
}

// `From` implementations for the pattern rule parser helper.
impl From<(Regex, bool)> for AclKeyRule {
    fn from((regex, is_allow): (Regex, bool)) -> Self {
        if is_allow {
            AclKeyRule::Allow(regex)
        } else {
            AclKeyRule::Deny(regex)
        }
    }
}

impl From<(Regex, bool)> for AclPubSubRule {
    fn from((regex, is_allow): (Regex, bool)) -> Self {
        if is_allow {
            AclPubSubRule::Allow(regex)
        } else {
            AclPubSubRule::Deny(regex)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AclConfig;
    use crate::core::acl::rules::AclRule;
    use bytes::Bytes;

    fn empty_config() -> AclConfig {
        AclConfig::default()
    }

    fn enabled_config_with(rules: Vec<AclRule>) -> AclConfig {
        AclConfig {
            enabled: true,
            users: vec![],
            rules,
        }
    }

    fn rule(name: &str, commands: &[&str]) -> AclRule {
        AclRule {
            name: name.to_string(),
            commands: Some(commands.iter().map(|s| s.to_string()).collect()),
            keys: None,
            pubsub_channels: None,
            conditions: vec![],
        }
    }

    fn user_with_rules(rules: &[&str]) -> AclUser {
        AclUser {
            username: "u".to_string(),
            password_hash: "h".to_string(),
            rules: rules.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_disabled_enforcer_allows_everything() {
        let cfg = empty_config();
        let e = AclEnforcer::new(&cfg);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"k"))];
        assert!(e.check_permission(
            None,
            &args,
            "GET",
            CommandFlags::READONLY,
            &["k".to_string()],
            &[]
        ));
    }

    #[test]
    fn test_enabled_enforcer_default_denies_when_user_has_no_rules() {
        // A user with no rules cannot run anything (default deny at the
        // command-permission level).
        let cfg = enabled_config_with(vec![]);
        let e = AclEnforcer::new(&cfg);
        let user = AclUser {
            username: "u".to_string(),
            password_hash: "h".to_string(),
            rules: vec![],
        };
        let args: Vec<RespFrame> = vec![];
        assert!(!e.check_permission(Some(&user), &args, "GET", CommandFlags::READONLY, &[], &[]));
    }

    #[test]
    fn test_explicit_allow_command() {
        let cfg = enabled_config_with(vec![rule("readonly", &["+get"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["readonly"]);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"k"))];
        // Pass empty keys to bypass the key-permission check.
        assert!(e.check_permission(Some(&u), &args, "GET", CommandFlags::READONLY, &[], &[]));
    }

    #[test]
    fn test_explicit_deny_command() {
        let cfg = enabled_config_with(vec![rule("norw", &["+@all", "-flushdb"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["norw"]);
        let args: Vec<RespFrame> = vec![];
        // +@all allows everything but -flushdb denies FLUSHDB.
        assert!(!e.check_permission(Some(&u), &args, "FLUSHDB", CommandFlags::ADMIN, &[], &[]));
    }

    #[test]
    fn test_allow_category_write() {
        let cfg = enabled_config_with(vec![rule("writer", &["+@write"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["writer"]);
        let args: Vec<RespFrame> = vec![];
        // Empty keys; just testing the command permission path.
        assert!(e.check_permission(Some(&u), &args, "SET", CommandFlags::WRITE, &[], &[]));
    }

    #[test]
    fn test_deny_category_admin() {
        let cfg = enabled_config_with(vec![rule("plain", &["+@read", "-@admin"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["plain"]);
        let args: Vec<RespFrame> = vec![];
        // CONFIG is an admin command, should be denied.
        assert!(!e.check_permission(Some(&u), &args, "CONFIG", CommandFlags::ADMIN, &[], &[]));
    }

    #[test]
    fn test_command_name_is_case_insensitive() {
        let cfg = enabled_config_with(vec![rule("r", &["+get"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"k"))];
        // Mixed-case command name still matches the rule.
        assert!(e.check_permission(Some(&u), &args, "Get", CommandFlags::READONLY, &[], &[]));
    }

    #[test]
    fn test_unprefixed_rule_defaults_to_allow() {
        let cfg = enabled_config_with(vec![rule("r", &["ping"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args: Vec<RespFrame> = vec![];
        assert!(e.check_permission(
            Some(&u),
            &args,
            "PING",
            CommandFlags::ADMIN | CommandFlags::READONLY,
            &[],
            &[]
        ));
    }

    #[test]
    fn test_user_with_unknown_rule_name_is_default_denied() {
        let cfg = enabled_config_with(vec![rule("r", &["+get"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["nonexistent"]);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"k"))];
        assert!(!e.check_permission(Some(&u), &args, "GET", CommandFlags::READONLY, &[], &[]));
    }

    #[test]
    fn test_no_user_default_denied_when_enabled() {
        let cfg = enabled_config_with(vec![rule("r", &["+get"])]);
        let e = AclEnforcer::new(&cfg);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"k"))];
        // No user supplied (None) → denied.
        assert!(!e.check_permission(None, &args, "GET", CommandFlags::READONLY, &[], &[]));
    }

    #[test]
    fn test_allow_all_via_plus_at_all() {
        let cfg = enabled_config_with(vec![rule("r", &["+@all"])]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args: Vec<RespFrame> = vec![];
        assert!(e.check_permission(Some(&u), &args, "ANYCMD", CommandFlags::empty(), &[], &[]));
    }

    #[test]
    fn test_invalid_glob_is_skipped() {
        // A bad pattern shouldn't crash; the rule is just dropped.
        let bad = AclRule {
            name: "r".to_string(),
            commands: Some(vec!["+@all".to_string()]),
            keys: Some(vec!["~[".to_string()]), // unclosed char class → invalid regex
            pubsub_channels: None,
            conditions: vec![],
        };
        let cfg = enabled_config_with(vec![bad]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args: Vec<RespFrame> = vec![];
        // Command permission still works even if the key pattern is invalid.
        assert!(e.check_permission(
            Some(&u),
            &args,
            "PING",
            CommandFlags::ADMIN | CommandFlags::READONLY,
            &[],
            &[]
        ));
    }

    fn rule_with_keys(name: &str, commands: &[&str], keys: &[&str]) -> AclRule {
        AclRule {
            name: name.to_string(),
            commands: Some(commands.iter().map(|s| s.to_string()).collect()),
            keys: Some(keys.iter().map(|s| s.to_string()).collect()),
            pubsub_channels: None,
            conditions: vec![],
        }
    }

    fn rule_with_channels(name: &str, commands: &[&str], channels: &[&str]) -> AclRule {
        AclRule {
            name: name.to_string(),
            commands: Some(commands.iter().map(|s| s.to_string()).collect()),
            keys: None,
            pubsub_channels: Some(channels.iter().map(|s| s.to_string()).collect()),
            conditions: vec![],
        }
    }

    #[test]
    fn test_key_glob_allow_matches() {
        let cfg = enabled_config_with(vec![rule_with_keys(
            "r",
            &["+get"],
            &["~user:*"],
        )]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"user:42"))];
        assert!(e.check_permission(
            Some(&u),
            &args,
            "GET",
            CommandFlags::READONLY,
            &["user:42".to_string()],
            &[]
        ));
    }

    #[test]
    fn test_key_glob_deny_takes_precedence() {
        let cfg = enabled_config_with(vec![rule_with_keys(
            "r",
            &["+@all"],
            &["~*", "-admin:*"],
        )]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"admin:secret"))];
        assert!(!e.check_permission(
            Some(&u),
            &args,
            "GET",
            CommandFlags::READONLY,
            &["admin:secret".to_string()],
            &[]
        ));
    }

    #[test]
    fn test_key_pattern_does_not_match_other_keys() {
        let cfg = enabled_config_with(vec![rule_with_keys(
            "r",
            &["+@all"],
            &["~user:*"],
        )]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"order:1"))];
        // `order:1` does not match `user:*` so it should be denied.
        assert!(!e.check_permission(
            Some(&u),
            &args,
            "GET",
            CommandFlags::READONLY,
            &["order:1".to_string()],
            &[]
        ));
    }

    #[test]
    fn test_allkeys_shortcut_allows_any_key() {
        let cfg = enabled_config_with(vec![rule_with_keys(
            "r",
            &["+@all"],
            &["allkeys"],
        )]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"anything"))];
        assert!(e.check_permission(
            Some(&u),
            &args,
            "GET",
            CommandFlags::READONLY,
            &["literally:any:key".to_string()],
            &[]
        ));
    }

    #[test]
    fn test_pubsub_glob_allow() {
        let cfg = enabled_config_with(vec![rule_with_channels(
            "r",
            &["+@all"],
            &["&news.*"],
        )]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args: Vec<RespFrame> = vec![];
        assert!(e.check_permission(
            Some(&u),
            &args,
            "PUBLISH",
            CommandFlags::PUBSUB | CommandFlags::WRITE,
            &[],
            &["news.weather".to_string()]
        ));
    }

    #[test]
    fn test_pubsub_deny_blocks_publish() {
        let cfg = enabled_config_with(vec![rule_with_channels(
            "r",
            &["+@all"],
            &["&news.*", "-internal.*"],
        )]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args: Vec<RespFrame> = vec![];
        assert!(!e.check_permission(
            Some(&u),
            &args,
            "PUBLISH",
            CommandFlags::PUBSUB | CommandFlags::WRITE,
            &[],
            &["internal.alerts".to_string()]
        ));
    }

    #[test]
    fn test_allchannels_shortcut_allows_any_channel() {
        let cfg = enabled_config_with(vec![rule_with_channels(
            "r",
            &["+@all"],
            &["allchannels"],
        )]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        let args: Vec<RespFrame> = vec![];
        assert!(e.check_permission(
            Some(&u),
            &args,
            "PUBLISH",
            CommandFlags::PUBSUB | CommandFlags::WRITE,
            &[],
            &["any.channel".to_string()]
        ));
    }

    #[test]
    fn test_auth_allowed_before_authentication() {
        // Even with ACL enabled, AUTH is allowed when no user is provided.
        let cfg = enabled_config_with(vec![rule("r", &["+@all"])]);
        let e = AclEnforcer::new(&cfg);
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"user"))];
        assert!(e.check_permission(
            None,
            &args,
            "AUTH",
            CommandFlags::ADMIN,
            &[],
            &[]
        ));
    }

    #[test]
    fn test_auth_case_insensitive() {
        // The special-case check is case-insensitive.
        let cfg = enabled_config_with(vec![rule("r", &["+@all"])]);
        let e = AclEnforcer::new(&cfg);
        let args: Vec<RespFrame> = vec![];
        assert!(e.check_permission(
            None,
            &args,
            "auth",
            CommandFlags::ADMIN,
            &[],
            &[]
        ));
    }

    #[test]
    fn test_condition_argc_less_than_grants_otherwise_denied_command() {
        use crate::core::acl::rules::{AclCondition, ConditionOperator, ConditionTarget};
        // Start default-deny; condition grants GET when argc < 3.
        let cond_rule = AclRule {
            name: "r".to_string(),
            commands: None,
            keys: None,
            pubsub_channels: None,
            conditions: vec![AclCondition {
                target: ConditionTarget::Command,
                operator: ConditionOperator::ArgcLessThan(3),
                result: vec!["+GET".to_string()],
            }],
        };
        let cfg = enabled_config_with(vec![cond_rule]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        // raw_args len is 1 (just the key) → argc = 2 < 3 → GET allowed.
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"k"))];
        assert!(e.check_permission(
            Some(&u),
            &args,
            "GET",
            CommandFlags::READONLY,
            &[],
            &[]
        ));
    }

    #[test]
    fn test_condition_argc_greater_than_does_not_trigger() {
        use crate::core::acl::rules::{AclCondition, ConditionOperator, ConditionTarget};
        let cond_rule = AclRule {
            name: "r".to_string(),
            commands: None,
            keys: None,
            pubsub_channels: None,
            conditions: vec![AclCondition {
                target: ConditionTarget::Command,
                operator: ConditionOperator::ArgcGreaterThan(5),
                result: vec!["+GET".to_string()],
            }],
        };
        let cfg = enabled_config_with(vec![cond_rule]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);
        // argc = 2, not greater than 5 → condition does not fire → denied.
        let args = vec![RespFrame::BulkString(Bytes::from_static(b"k"))];
        assert!(!e.check_permission(
            Some(&u),
            &args,
            "GET",
            CommandFlags::READONLY,
            &[],
            &[]
        ));
    }

    #[test]
    fn test_condition_starts_with_on_arg() {
        use crate::core::acl::rules::{AclCondition, ConditionOperator, ConditionTarget};
        // Condition grants SET only when arg[0] starts with "user:".
        let cond_rule = AclRule {
            name: "r".to_string(),
            commands: None,
            keys: None,
            pubsub_channels: None,
            conditions: vec![AclCondition {
                target: ConditionTarget::Arg { index: 0 },
                operator: ConditionOperator::StartsWith("user:".to_string()),
                result: vec!["+SET".to_string()],
            }],
        };
        let cfg = enabled_config_with(vec![cond_rule]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);

        // arg[0] = "user:42" → matches → SET allowed.
        let args_match = vec![RespFrame::BulkString(Bytes::from_static(b"user:42"))];
        assert!(e.check_permission(
            Some(&u),
            &args_match,
            "SET",
            CommandFlags::WRITE,
            &[],
            &[]
        ));

        // arg[0] = "other:1" → no match → denied.
        let args_nomatch = vec![RespFrame::BulkString(Bytes::from_static(b"other:1"))];
        assert!(!e.check_permission(
            Some(&u),
            &args_nomatch,
            "SET",
            CommandFlags::WRITE,
            &[],
            &[]
        ));
    }

    #[test]
    fn test_condition_is_number_on_arg() {
        use crate::core::acl::rules::{AclCondition, ConditionOperator, ConditionTarget};
        let cond_rule = AclRule {
            name: "r".to_string(),
            commands: None,
            keys: None,
            pubsub_channels: None,
            conditions: vec![AclCondition {
                target: ConditionTarget::Arg { index: 0 },
                operator: ConditionOperator::IsNumber,
                result: vec!["+INCRBY".to_string()],
            }],
        };
        let cfg = enabled_config_with(vec![cond_rule]);
        let e = AclEnforcer::new(&cfg);
        let u = user_with_rules(&["r"]);

        // "42" is a number → INCRBY allowed.
        let args_num = vec![RespFrame::BulkString(Bytes::from_static(b"42"))];
        assert!(e.check_permission(
            Some(&u),
            &args_num,
            "INCRBY",
            CommandFlags::WRITE,
            &[],
            &[]
        ));

        // "notanumber" is not a number → INCRBY denied.
        let args_str = vec![RespFrame::BulkString(Bytes::from_static(b"notanumber"))];
        assert!(!e.check_permission(
            Some(&u),
            &args_str,
            "INCRBY",
            CommandFlags::WRITE,
            &[],
            &[]
        ));
    }
}
