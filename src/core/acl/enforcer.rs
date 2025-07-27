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
