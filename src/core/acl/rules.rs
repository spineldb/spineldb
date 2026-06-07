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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_condition_target_key_deserializes_with_index() {
        // The `key` variant is tagged by `on = "key"` and includes an `index`.
        let json = r#"{"on":"key","index":2,"operator":{"equals":"x"}}"#;
        let t: ConditionTarget = serde_json::from_str(json).unwrap();
        match t {
            ConditionTarget::Key { index } => assert_eq!(index, 2),
            other => panic!("expected Key, got {other:?}"),
        }
    }

    #[test]
    fn test_condition_target_arg_deserializes_with_index() {
        let json = r#"{"on":"arg","index":0,"operator":{"is-number":null}}"#;
        let t: ConditionTarget = serde_json::from_str(json).unwrap();
        match t {
            ConditionTarget::Arg { index } => assert_eq!(index, 0),
            other => panic!("expected Arg, got {other:?}"),
        }
    }

    #[test]
    fn test_condition_target_command_deserializes() {
        let json = r#"{"on":"command","operator":{"argc-less-than":3}}"#;
        let t: ConditionTarget = serde_json::from_str(json).unwrap();
        assert!(matches!(t, ConditionTarget::Command));
    }

    #[test]
    fn test_condition_operator_starts_with() {
        // Internally-tagged: tag is `operator`, value lives under `value`.
        let json = r#"{"operator":"starts-with","value":"SET"}"#;
        let op: ConditionOperator = serde_json::from_str(json).unwrap();
        assert!(matches!(op, ConditionOperator::StartsWith(s) if s == "SET"));
    }

    #[test]
    fn test_condition_operator_equals() {
        let json = r#"{"operator":"equals","value":"X"}"#;
        let op: ConditionOperator = serde_json::from_str(json).unwrap();
        assert!(matches!(op, ConditionOperator::Equals(s) if s == "X"));
    }

    #[test]
    fn test_condition_operator_is_number() {
        let json = r#"{"operator":"is-number","value":null}"#;
        let op: ConditionOperator = serde_json::from_str(json).unwrap();
        assert!(matches!(op, ConditionOperator::IsNumber));
    }

    #[test]
    fn test_condition_operator_argc_less_than() {
        let json = r#"{"operator":"argc-less-than","value":4}"#;
        let op: ConditionOperator = serde_json::from_str(json).unwrap();
        assert!(matches!(op, ConditionOperator::ArgcLessThan(4)));
    }

    #[test]
    fn test_condition_operator_argc_greater_than() {
        let json = r#"{"operator":"argc-greater-than","value":2}"#;
        let op: ConditionOperator = serde_json::from_str(json).unwrap();
        assert!(matches!(op, ConditionOperator::ArgcGreaterThan(2)));
    }

    #[test]
    fn test_condition_operator_unknown_variant_is_error() {
        let json = r#"{"operator":"nope","value":"x"}"#;
        let r: Result<ConditionOperator, _> = serde_json::from_str(json);
        assert!(r.is_err());
    }

    #[test]
    fn test_acl_rule_minimal_deserializes() {
        let json = r#"{"name":"readonly","commands":["+@read"]}"#;
        let r: AclRule = serde_json::from_str(json).unwrap();
        assert_eq!(r.name, "readonly");
        assert_eq!(r.commands.as_deref(), Some(&["+@read".to_string()][..]));
        assert!(r.keys.is_none());
        assert!(r.pubsub_channels.is_none());
        assert!(r.conditions.is_empty());
    }

    #[test]
    fn test_acl_rule_full_deserializes() {
        let json = r#"{
            "name":"writers",
            "commands":["+@write","-DEBUG"],
            "keys":["~user:*","~order:*"],
            "pubsub_channels":["&news.*"],
            "conditions":[
                {
                    "target":{"on":"command"},
                    "operator":{"operator":"argc-greater-than","value":1},
                    "result":["+SET","+DEL"]
                }
            ]
        }"#;
        let r: AclRule = serde_json::from_str(json).unwrap();
        assert_eq!(r.name, "writers");
        assert_eq!(r.commands.as_ref().unwrap().len(), 2);
        assert_eq!(r.keys.as_ref().unwrap().len(), 2);
        assert_eq!(r.pubsub_channels.as_ref().unwrap().len(), 1);
        assert_eq!(r.conditions.len(), 1);
        assert!(matches!(
            r.conditions[0].target,
            ConditionTarget::Command
        ));
        assert_eq!(r.conditions[0].result, vec!["+SET", "+DEL"]);
    }

    #[test]
    fn test_acl_rule_missing_name_is_error() {
        let json = r#"{"commands":["+@read"]}"#;
        let r: Result<AclRule, _> = serde_json::from_str(json);
        assert!(r.is_err());
    }

    #[test]
    fn test_acl_rule_conditions_default_to_empty() {
        // Conditions are #[serde(default)]; absent key must yield empty Vec.
        let json = r#"{"name":"r","commands":[]}"#;
        let r: AclRule = serde_json::from_str(json).unwrap();
        assert!(r.conditions.is_empty());
    }

    #[test]
    fn test_acl_rule_serde_roundtrip() {
        let original = AclRule {
            name: "rt".to_string(),
            commands: Some(vec!["+GET".to_string(), "-DEL".to_string()]),
            keys: Some(vec!["~*".to_string()]),
            pubsub_channels: None,
            conditions: vec![AclCondition {
                target: ConditionTarget::Command,
                operator: ConditionOperator::IsNumber,
                result: vec!["+INCR".to_string()],
            }],
        };
        let s = serde_json::to_string(&original).unwrap();
        let parsed: AclRule = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed.name, original.name);
        assert_eq!(parsed.commands, original.commands);
        assert_eq!(parsed.keys, original.keys);
        assert_eq!(parsed.conditions.len(), 1);
        assert!(matches!(
            parsed.conditions[0].operator,
            ConditionOperator::IsNumber
        ));
    }
}
