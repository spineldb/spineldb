// src/core/acl/parsed_rules.rs

use crate::core::commands::command_trait::CommandFlags;
use regex::Regex;

/// An internal, efficient representation of a command permission rule.
#[derive(Debug, Clone)]
pub enum AclCommandRule {
    Allow(String),
    Deny(String),
    AllowCategory(CommandFlags),
    DenyCategory(CommandFlags),
    All,
}

/// An internal, efficient representation of a key pattern rule.
#[derive(Debug, Clone)]
pub enum AclKeyRule {
    Allow(Regex),
    Deny(Regex),
    All,
}

/// An internal, efficient representation of a Pub/Sub channel pattern rule.
#[derive(Debug, Clone)]
pub enum AclPubSubRule {
    Allow(Regex),
    Deny(Regex),
    All,
}

/// An internal, parsed representation of a dynamic `AclCondition`.
#[derive(Debug, Clone)]
pub struct ParsedAclCondition {
    pub target: crate::core::acl::rules::ConditionTarget,
    pub operator: crate::core::acl::rules::ConditionOperator,
    pub rules_on_match: Vec<AclCommandRule>,
}

/// Represents the internal, parsed representation of an AclRule.
/// This is more efficient to check against repeatedly.
#[derive(Debug, Clone, Default)]
pub struct ParsedAclRule {
    pub name: String,
    pub commands: Vec<AclCommandRule>,
    pub keys: Vec<AclKeyRule>,
    pub pubsub_channels: Vec<AclPubSubRule>,
    pub conditions: Vec<ParsedAclCondition>,
}
