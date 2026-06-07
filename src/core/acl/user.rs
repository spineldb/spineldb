// src/core/acl/user.rs

use serde::{Deserialize, Serialize};

/// Represents a single user in the ACL system.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AclUser {
    /// The username.
    pub username: String,
    /// The Argon2 password hash.
    pub password_hash: String,
    /// A list of rule names this user has access to.
    pub rules: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_user_minimal_deserializes() {
        let json = r#"{
            "username":"alice",
            "password_hash":"$argon2id$abc",
            "rules":["readers","writers"]
        }"#;
        let u: AclUser = serde_json::from_str(json).unwrap();
        assert_eq!(u.username, "alice");
        assert_eq!(u.password_hash, "$argon2id$abc");
        assert_eq!(u.rules, vec!["readers", "writers"]);
    }

    #[test]
    fn test_acl_user_empty_rules_is_allowed() {
        // An empty `rules` vec is valid (user with no extra rules).
        let json = r#"{"username":"x","password_hash":"h","rules":[]}"#;
        let u: AclUser = serde_json::from_str(json).unwrap();
        assert!(u.rules.is_empty());
    }

    #[test]
    fn test_acl_user_missing_username_is_error() {
        let json = r#"{"password_hash":"h","rules":[]}"#;
        let r: Result<AclUser, _> = serde_json::from_str(json);
        assert!(r.is_err());
    }

    #[test]
    fn test_acl_user_missing_password_hash_is_error() {
        let json = r#"{"username":"x","rules":[]}"#;
        let r: Result<AclUser, _> = serde_json::from_str(json);
        assert!(r.is_err());
    }

    #[test]
    fn test_acl_user_missing_rules_is_error() {
        let json = r#"{"username":"x","password_hash":"h"}"#;
        let r: Result<AclUser, _> = serde_json::from_str(json);
        assert!(r.is_err());
    }

    #[test]
    fn test_acl_user_clone_preserves_fields() {
        let u = AclUser {
            username: "bob".to_string(),
            password_hash: "h".to_string(),
            rules: vec!["r".to_string()],
        };
        let c = u.clone();
        assert_eq!(c.username, "bob");
        assert_eq!(c.password_hash, "h");
        assert_eq!(c.rules, vec!["r"]);
    }

    #[test]
    fn test_acl_user_serde_roundtrip() {
        let original = AclUser {
            username: "carol".to_string(),
            password_hash: "$argon2id$xyz".to_string(),
            rules: vec!["a".to_string(), "b".to_string()],
        };
        let s = serde_json::to_string(&original).unwrap();
        let parsed: AclUser = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed.username, original.username);
        assert_eq!(parsed.password_hash, original.password_hash);
        assert_eq!(parsed.rules, original.rules);
    }
}
