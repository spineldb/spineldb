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
