// tests/integration/acl_test.rs

//! Integration tests for Access Control List (ACL) functionality

use super::test_helpers::TestContext;
use bytes::Bytes;
use spineldb::config::Config;
use spineldb::core::Command;
use spineldb::core::RespValue;
use spineldb::core::SpinelDBError;
use spineldb::core::commands::command_trait::CommandFlags;
use spineldb::core::protocol::RespFrame;
use tempfile::TempDir;

/// Helper to execute an ACL command
async fn execute_acl(
    ctx: &TestContext,
    subcommand: &str,
    args: Vec<&str>,
) -> Result<RespValue, SpinelDBError> {
    let mut frames = vec![
        RespFrame::BulkString(Bytes::from_static(b"ACL")),
        RespFrame::BulkString(Bytes::from(subcommand.to_string())),
    ];
    for arg in args {
        frames.push(RespFrame::BulkString(Bytes::from(arg.to_string())));
    }
    let command = Command::try_from(RespFrame::Array(frames))?;
    ctx.execute(command).await
}

/// Helper to execute ACL SETUSER
async fn acl_setuser(
    ctx: &TestContext,
    username: &str,
    rules: Vec<&str>,
) -> Result<RespValue, SpinelDBError> {
    let mut args = vec![username];
    args.extend(rules);
    execute_acl(ctx, "SETUSER", args).await
}

/// Helper to execute ACL GETUSER
async fn acl_getuser(ctx: &TestContext, username: &str) -> Result<RespValue, SpinelDBError> {
    execute_acl(ctx, "GETUSER", vec![username]).await
}

/// Helper to execute ACL DELUSER
async fn acl_deluser(ctx: &TestContext, username: &str) -> Result<RespValue, SpinelDBError> {
    execute_acl(ctx, "DELUSER", vec![username]).await
}

/// Helper to execute ACL LIST
async fn acl_list(ctx: &TestContext) -> Result<RespValue, SpinelDBError> {
    execute_acl(ctx, "LIST", vec![]).await
}

/// Helper to execute ACL SAVE
async fn acl_save(ctx: &TestContext) -> Result<RespValue, SpinelDBError> {
    execute_acl(ctx, "SAVE", vec![]).await
}

// ===== ACL SETUSER Tests =====

#[tokio::test]
async fn test_acl_setuser_create_new_user() {
    let ctx = TestContext::new().await;

    // Create a new user with password and rules
    let result = acl_setuser(&ctx, "testuser", vec![">password123", "+GET", "+SET"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify user was created
    let user_result = acl_getuser(&ctx, "testuser").await.unwrap();
    match user_result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString("rules".into()));
            let rules_str = match &arr[1] {
                RespValue::BulkString(bs) => String::from_utf8_lossy(bs),
                _ => panic!("Expected bulk string for rules"),
            };
            assert!(rules_str.contains("GET"));
            assert!(rules_str.contains("SET"));
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_acl_setuser_update_existing_user() {
    let ctx = TestContext::new().await;

    // Create initial user
    acl_setuser(&ctx, "updateuser", vec![">oldpass", "+GET"])
        .await
        .unwrap();

    // Update user with new rules
    let result = acl_setuser(&ctx, "updateuser", vec![">newpass", "+SET", "+DEL"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify updated rules
    let user_result = acl_getuser(&ctx, "updateuser").await.unwrap();
    match user_result {
        RespValue::Array(arr) => {
            let rules_str = match &arr[1] {
                RespValue::BulkString(bs) => String::from_utf8_lossy(bs),
                _ => panic!("Expected bulk string"),
            };
            assert!(rules_str.contains("SET"));
            assert!(rules_str.contains("DEL"));
            assert!(!rules_str.contains("GET")); // Old rule should be replaced
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_acl_setuser_enable_acl() {
    let ctx = TestContext::new().await;

    // Enable ACL and create user
    let result = acl_setuser(&ctx, "admin", vec![">adminpass", "on", "+@all"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify ACL is enabled
    let acl_config = ctx.state.acl_config.read().await;
    assert!(acl_config.enabled);
}

#[tokio::test]
async fn test_acl_setuser_disable_acl() {
    let ctx = TestContext::new().await;

    // First enable ACL
    acl_setuser(&ctx, "user1", vec![">pass1", "on", "+GET"])
        .await
        .unwrap();

    // Disable ACL
    acl_setuser(&ctx, "user2", vec![">pass2", "off"])
        .await
        .unwrap();

    // Verify ACL is disabled
    let acl_config = ctx.state.acl_config.read().await;
    assert!(!acl_config.enabled);
}

#[tokio::test]
async fn test_acl_setuser_new_user_requires_password() {
    let ctx = TestContext::new().await;

    // Try to create user without password
    let result = acl_setuser(&ctx, "nopass", vec!["+GET"]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        SpinelDBError::InvalidState(msg) => {
            assert!(msg.contains("Password must be provided"));
        }
        _ => panic!("Expected InvalidState error"),
    }
}

#[tokio::test]
async fn test_acl_setuser_with_category_rules() {
    let ctx = TestContext::new().await;

    // Create user with category rules
    let result = acl_setuser(&ctx, "readonly", vec![">readpass", "+@read", "-@write"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify rules
    let user_result = acl_getuser(&ctx, "readonly").await.unwrap();
    match user_result {
        RespValue::Array(arr) => {
            let rules_str = match &arr[1] {
                RespValue::BulkString(bs) => String::from_utf8_lossy(bs),
                _ => panic!("Expected bulk string"),
            };
            assert!(rules_str.contains("@read"));
            assert!(rules_str.contains("@write"));
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_acl_setuser_with_key_patterns() {
    let ctx = TestContext::new().await;

    // Create user with key pattern rules
    let result = acl_setuser(
        &ctx,
        "restricted",
        vec![">restpass", "+@all", "~user:*", "~app:*"],
    )
    .await
    .unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify user was created
    let user_result = acl_getuser(&ctx, "restricted").await.unwrap();
    assert!(!matches!(user_result, RespValue::Null));
}

#[tokio::test]
async fn test_acl_setuser_update_password_only() {
    let ctx = TestContext::new().await;

    // Create user
    acl_setuser(&ctx, "passuser", vec![">oldpass", "+GET"])
        .await
        .unwrap();

    // Update only password
    let result = acl_setuser(&ctx, "passuser", vec![">newpass"])
        .await
        .unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // User should still exist
    let user_result = acl_getuser(&ctx, "passuser").await.unwrap();
    assert!(!matches!(user_result, RespValue::Null));
}

// ===== ACL GETUSER Tests =====

#[tokio::test]
async fn test_acl_getuser_existing_user() {
    let ctx = TestContext::new().await;

    // Create user
    acl_setuser(&ctx, "getuser", vec![">getpass", "+GET", "+SET"])
        .await
        .unwrap();

    // Get user
    let result = acl_getuser(&ctx, "getuser").await.unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString("rules".into()));
        }
        _ => panic!("Expected array response"),
    }
}

#[tokio::test]
async fn test_acl_getuser_nonexistent_user() {
    let ctx = TestContext::new().await;

    // Get non-existent user
    let result = acl_getuser(&ctx, "nonexistent").await.unwrap();
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_acl_getuser_multiple_users() {
    let ctx = TestContext::new().await;

    // Create multiple users
    acl_setuser(&ctx, "user1", vec![">pass1", "+GET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "user2", vec![">pass2", "+SET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "user3", vec![">pass3", "+DEL"])
        .await
        .unwrap();

    // Get each user
    let u1 = acl_getuser(&ctx, "user1").await.unwrap();
    let u2 = acl_getuser(&ctx, "user2").await.unwrap();
    let u3 = acl_getuser(&ctx, "user3").await.unwrap();

    assert!(!matches!(u1, RespValue::Null));
    assert!(!matches!(u2, RespValue::Null));
    assert!(!matches!(u3, RespValue::Null));
}

// ===== ACL DELUSER Tests =====

#[tokio::test]
async fn test_acl_deluser_existing_user() {
    let ctx = TestContext::new().await;

    // Create user
    acl_setuser(&ctx, "deluser", vec![">delpass", "+GET"])
        .await
        .unwrap();

    // Delete user
    let result = acl_deluser(&ctx, "deluser").await.unwrap();
    match result {
        RespValue::Integer(1) => {} // User was deleted
        _ => panic!("Expected Integer(1)"),
    }

    // Verify user is gone
    let user_result = acl_getuser(&ctx, "deluser").await.unwrap();
    assert_eq!(user_result, RespValue::Null);
}

#[tokio::test]
async fn test_acl_deluser_nonexistent_user() {
    let ctx = TestContext::new().await;

    // Delete non-existent user
    let result = acl_deluser(&ctx, "nonexistent").await.unwrap();
    match result {
        RespValue::Integer(0) => {} // User was not found
        _ => panic!("Expected Integer(0)"),
    }
}

#[tokio::test]
async fn test_acl_deluser_multiple_users() {
    let ctx = TestContext::new().await;

    // Create multiple users
    acl_setuser(&ctx, "multi1", vec![">pass1", "+GET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "multi2", vec![">pass2", "+SET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "multi3", vec![">pass3", "+DEL"])
        .await
        .unwrap();

    // Delete one user
    let result = acl_deluser(&ctx, "multi2").await.unwrap();
    assert_eq!(result, RespValue::Integer(1));

    // Verify only multi2 is gone
    assert!(!matches!(
        acl_getuser(&ctx, "multi1").await.unwrap(),
        RespValue::Null
    ));
    assert_eq!(acl_getuser(&ctx, "multi2").await.unwrap(), RespValue::Null);
    assert!(!matches!(
        acl_getuser(&ctx, "multi3").await.unwrap(),
        RespValue::Null
    ));
}

// ===== ACL LIST Tests =====

#[tokio::test]
async fn test_acl_list_empty() {
    let ctx = TestContext::new().await;

    // List users when none exist
    let result = acl_list(&ctx).await.unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 0);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_acl_list_single_user() {
    let ctx = TestContext::new().await;

    // Create user
    acl_setuser(&ctx, "listuser", vec![">listpass", "+GET", "+SET"])
        .await
        .unwrap();

    // List users
    let result = acl_list(&ctx).await.unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                RespValue::BulkString(bs) => {
                    let line = String::from_utf8_lossy(bs);
                    assert!(line.contains("listuser"));
                    assert!(line.contains("GET"));
                    assert!(line.contains("SET"));
                }
                _ => panic!("Expected bulk string"),
            }
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_acl_list_multiple_users() {
    let ctx = TestContext::new().await;

    // Create multiple users
    acl_setuser(&ctx, "list1", vec![">pass1", "+GET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "list2", vec![">pass2", "+SET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "list3", vec![">pass3", "+DEL"])
        .await
        .unwrap();

    // List users
    let result = acl_list(&ctx).await.unwrap();
    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            // Verify all users are present
            let lines: Vec<String> = arr
                .iter()
                .map(|v| match v {
                    RespValue::BulkString(bs) => String::from_utf8_lossy(bs).to_string(),
                    _ => String::new(),
                })
                .collect();
            assert!(lines.iter().any(|l| l.contains("list1")));
            assert!(lines.iter().any(|l| l.contains("list2")));
            assert!(lines.iter().any(|l| l.contains("list3")));
        }
        _ => panic!("Expected array"),
    }
}

// ===== ACL SAVE Tests =====

#[tokio::test]
async fn test_acl_save_without_acl_file() {
    let ctx = TestContext::new().await;

    // Create a user
    acl_setuser(&ctx, "saveuser", vec![">savepass", "+GET"])
        .await
        .unwrap();

    // Try to save without acl_file configured
    let result = acl_save(&ctx).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        SpinelDBError::InvalidState(msg) => {
            assert!(msg.contains("ACL file not configured"));
        }
        _ => panic!("Expected InvalidState error"),
    }
}

#[tokio::test]
async fn test_acl_save_with_acl_file() {
    // Create temp directory for ACL file
    let temp_dir = TempDir::new().unwrap();
    let acl_file_path = temp_dir.path().join("users.json");

    // Create config with acl_file
    let mut config = Config::default();
    config.databases = 1;
    config.persistence.aof_enabled = false;
    config.persistence.spldb_enabled = false;
    config.acl_file = Some(acl_file_path.to_string_lossy().to_string());

    let ctx = TestContext::with_config(config).await;

    // Create users
    acl_setuser(&ctx, "save1", vec![">pass1", "+GET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "save2", vec![">pass2", "+SET"])
        .await
        .unwrap();

    // Save ACL
    let result = acl_save(&ctx).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));

    // Verify file was created
    assert!(acl_file_path.exists());

    // Verify file contents (basic check)
    let contents = std::fs::read_to_string(&acl_file_path).unwrap();
    assert!(contents.contains("save1"));
    assert!(contents.contains("save2"));
}

#[tokio::test]
async fn test_acl_save_preserves_users() {
    let temp_dir = TempDir::new().unwrap();
    let acl_file_path = temp_dir.path().join("users.json");

    let mut config = Config::default();
    config.databases = 1;
    config.persistence.aof_enabled = false;
    config.persistence.spldb_enabled = false;
    config.acl_file = Some(acl_file_path.to_string_lossy().to_string());

    let ctx = TestContext::with_config(config).await;

    // Create and save users
    acl_setuser(&ctx, "preserve1", vec![">pass1", "+GET"])
        .await
        .unwrap();
    acl_setuser(&ctx, "preserve2", vec![">pass2", "+SET"])
        .await
        .unwrap();
    acl_save(&ctx).await.unwrap();

    // Add more users
    acl_setuser(&ctx, "preserve3", vec![">pass3", "+DEL"])
        .await
        .unwrap();
    acl_save(&ctx).await.unwrap();

    // Verify all users are in file
    let contents = std::fs::read_to_string(&acl_file_path).unwrap();
    assert!(contents.contains("preserve1"));
    assert!(contents.contains("preserve2"));
    assert!(contents.contains("preserve3"));
}

// ===== ACL Enforcement Tests =====
// Note: These tests verify the enforcer behavior when ACL is enabled/disabled
// The enforcer requires named rules from config, so we test basic scenarios

#[tokio::test]
async fn test_acl_enforcer_disabled_allows_all() {
    let ctx = TestContext::new().await;

    // Don't enable ACL (default is disabled)
    let raw_args = vec![
        RespFrame::BulkString(Bytes::from("GET")),
        RespFrame::BulkString(Bytes::from("key")),
    ];

    let enforcer = ctx.state.acl_enforcer.read().await;

    // When ACL is disabled, all commands should be allowed
    let allowed = enforcer.check_permission(
        None,
        &raw_args,
        "GET",
        CommandFlags::READONLY,
        &["key".to_string()],
        &[],
    );

    assert!(allowed);
}

#[tokio::test]
async fn test_acl_enforcer_denies_unauthenticated_user_when_enabled() {
    let ctx = TestContext::new().await;

    // Enable ACL
    acl_setuser(&ctx, "user", vec![">pass", "on", "+@all"])
        .await
        .unwrap();

    let raw_args = vec![
        RespFrame::BulkString(Bytes::from("GET")),
        RespFrame::BulkString(Bytes::from("key")),
    ];

    let enforcer = ctx.state.acl_enforcer.read().await;

    // Should deny when user is None and ACL is enabled
    let allowed = enforcer.check_permission(
        None,
        &raw_args,
        "GET",
        CommandFlags::READONLY,
        &["key".to_string()],
        &[],
    );

    assert!(!allowed);
}

#[tokio::test]
async fn test_acl_enforcer_allows_auth_command() {
    let ctx = TestContext::new().await;

    // Enable ACL
    acl_setuser(&ctx, "user", vec![">pass", "on", "+@all"])
        .await
        .unwrap();

    let raw_args = vec![
        RespFrame::BulkString(Bytes::from("AUTH")),
        RespFrame::BulkString(Bytes::from("user")),
        RespFrame::BulkString(Bytes::from("pass")),
    ];

    let enforcer = ctx.state.acl_enforcer.read().await;

    // AUTH command should be allowed even without authenticated user
    let allowed =
        enforcer.check_permission(None, &raw_args, "AUTH", CommandFlags::empty(), &[], &[]);

    assert!(allowed);
}
