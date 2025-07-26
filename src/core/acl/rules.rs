// src/core/acl/rules.rs

use serde::{Deserialize, Serialize};

/// Specifies the target of a condition (e.g., a key or an argument).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", tag = "on")]
pub enum ConditionTarget {
    /// Condition applies to a command key.
    Key { index: usize },
    /// Condition applies to a command argument.
    Arg { index: usize },
    /// Condition applies to properties of the command itself.
    Command,
}

/// Specifies the comparison operator for a condition.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", tag = "operator", content = "value")]
pub enum ConditionOperator {
    /// Checks if a string starts with a given prefix.
    StartsWith(String),
    /// Checks if a string is exactly equal to a value.
    Equals(String),
    /// Checks if an argument can be parsed as a number.
    IsNumber,
    /// Checks if the argument count (including command name) is less than a value.
    ArgcLessThan(usize),
    /// Checks if the argument count (including command name) is greater than a value.
    ArgcGreaterThan(usize),
}

/// A single, evaluatable condition within an ACL rule.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AclCondition {
    /// The target of the condition (what is being checked).
    pub target: ConditionTarget,
    /// The comparison operator to use.
    pub operator: ConditionOperator,
    /// The rule(s) to apply if this condition is true (e.g., "+@write", "-DEL").
    pub result: Vec<String>,
}

/// Represents a single ACL rule from the configuration file.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AclRule {
    /// The unique name of the rule.
    pub name: String,
    /// Static command permissions (+COMMAND, -COMMAND, +@category, -@category).
    pub commands: Option<Vec<String>>,
    /// Key pattern permissions (~key*, allkeys).
    pub keys: Option<Vec<String>>,
    /// Pub/Sub channel pattern permissions (&channel*, allchannels).
    pub pubsub_channels: Option<Vec<String>>,
    /// A list of dynamic conditions to evaluate for this rule.
    #[serde(default)]
    pub conditions: Vec<AclCondition>,
}
