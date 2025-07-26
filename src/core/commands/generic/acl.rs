// src/core/commands/generic/acl.rs

use crate::config::AclUsersFile;
use crate::core::acl::enforcer::AclEnforcer;
use crate::core::acl::user::AclUser;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHasher};
use async_trait::async_trait;
use bytes::Bytes;
use rand::rngs::OsRng;
use std::sync::Arc;
use tracing::info;

/// Enum for ACL subcommands.
#[derive(Debug, Clone, Default)]
pub enum AclSubcommand {
    SetUser {
        username: String,
        rules: Vec<String>,
    },
    GetUser(String),
    DelUser(String),
    #[default]
    List,
    Save,
}

/// The main ACL command struct.
#[derive(Debug, Clone, Default)]
pub struct Acl {
    pub subcommand: AclSubcommand,
}

impl ParseCommand for Acl {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("ACL".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let subcommand = match sub_str.as_str() {
            "setuser" => {
                if args.len() < 2 {
                    return Err(SpinelDBError::WrongArgumentCount("ACL SETUSER".to_string()));
                }
                let username = extract_string(&args[1])?;
                let mut rules = Vec::new();
                for arg in args.iter().skip(2) {
                    if let RespFrame::BulkString(bytes) = arg {
                        rules.push(String::from_utf8(bytes.to_vec())?);
                    } else {
                        return Err(SpinelDBError::InvalidState(
                            "ACL rules must be bulk strings".to_string(),
                        ));
                    }
                }
                AclSubcommand::SetUser { username, rules }
            }
            "getuser" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount("ACL GETUSER".to_string()));
                }
                AclSubcommand::GetUser(extract_string(&args[1])?)
            }
            "deluser" => {
                if args.len() != 2 {
                    return Err(SpinelDBError::WrongArgumentCount("ACL DELUSER".to_string()));
                }
                AclSubcommand::DelUser(extract_string(&args[1])?)
            }
            "list" => {
                if args.len() > 1 {
                    return Err(SpinelDBError::WrongArgumentCount("ACL LIST".to_string()));
                }
                AclSubcommand::List
            }
            "save" => {
                if args.len() > 1 {
                    return Err(SpinelDBError::WrongArgumentCount("ACL SAVE".to_string()));
                }
                AclSubcommand::Save
            }
            _ => {
                return Err(SpinelDBError::UnknownCommand(
                    "ACL unknown subcommand".to_string(),
                ));
            }
        };

        Ok(Acl { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Acl {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            AclSubcommand::SetUser { username, rules } => {
                handle_setuser(ctx, username.clone(), rules.clone()).await
            }
            AclSubcommand::GetUser(username) => handle_getuser(ctx, username).await,
            AclSubcommand::DelUser(username) => handle_deluser(ctx, username).await,
            AclSubcommand::List => handle_list(ctx).await,
            AclSubcommand::Save => handle_save(ctx).await,
        }
    }
}

impl CommandSpec for Acl {
    fn name(&self) -> &'static str {
        "acl"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::NO_PROPAGATE
    }
    fn first_key(&self) -> i64 {
        0
    }
    fn last_key(&self) -> i64 {
        0
    }
    fn step(&self) -> i64 {
        0
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![]
    }
}

async fn handle_setuser(
    ctx: &mut ExecutionContext<'_>,
    username: String,
    rules: Vec<String>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let mut config_acl_guard = ctx.state.acl_config.write().await;

    let mut password_hash = String::new();
    let mut final_rules = Vec::new();

    for rule_part in rules {
        if let Some(pass) = rule_part.strip_prefix('>') {
            let salt = SaltString::generate(&mut OsRng);
            let argon2 = Argon2::default();
            password_hash = argon2
                .hash_password(pass.as_bytes(), &salt)
                .map_err(|_| SpinelDBError::Internal("Password hashing failed".to_string()))?
                .to_string();
        } else if rule_part == "on" {
            Arc::make_mut(&mut config_acl_guard).enabled = true;
        } else if rule_part == "off" {
            Arc::make_mut(&mut config_acl_guard).enabled = false;
        } else {
            final_rules.push(rule_part);
        }
    }

    if let Some(user) = Arc::make_mut(&mut config_acl_guard)
        .users
        .iter_mut()
        .find(|u| u.username == username)
    {
        if !password_hash.is_empty() {
            user.password_hash = password_hash;
        }
        user.rules = final_rules;
    } else {
        if password_hash.is_empty() {
            return Err(SpinelDBError::InvalidState(
                "Password must be provided for new users.".to_string(),
            ));
        }

        Arc::make_mut(&mut config_acl_guard).users.push(AclUser {
            username,
            password_hash,
            rules: final_rules,
        });
    }

    let new_enforcer = Arc::new(AclEnforcer::new(&config_acl_guard));
    *ctx.state.acl_enforcer.write().await = new_enforcer;

    Ok((
        RespValue::SimpleString("OK".into()),
        WriteOutcome::DidNotWrite,
    ))
}

async fn handle_getuser(
    ctx: &mut ExecutionContext<'_>,
    username: &str,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let config_acl_guard = ctx.state.acl_config.read().await;
    if let Some(user) = config_acl_guard
        .users
        .iter()
        .find(|u| u.username == username)
    {
        let response = vec![
            RespValue::BulkString("rules".into()),
            RespValue::BulkString(user.rules.join(" ").into()),
        ];
        Ok((RespValue::Array(response), WriteOutcome::DidNotWrite))
    } else {
        Ok((RespValue::Null, WriteOutcome::DidNotWrite))
    }
}

async fn handle_deluser(
    ctx: &mut ExecutionContext<'_>,
    username: &str,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let mut config_acl_guard = ctx.state.acl_config.write().await;

    let initial_len = config_acl_guard.users.len();
    Arc::make_mut(&mut config_acl_guard)
        .users
        .retain(|u| u.username != username);

    if config_acl_guard.users.len() < initial_len {
        let new_enforcer = Arc::new(AclEnforcer::new(&config_acl_guard));
        *ctx.state.acl_enforcer.write().await = new_enforcer;
        Ok((RespValue::Integer(1), WriteOutcome::DidNotWrite))
    } else {
        Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
    }
}

async fn handle_list(
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let config_acl_guard = ctx.state.acl_config.read().await;
    let list: Vec<RespValue> = config_acl_guard
        .users
        .iter()
        .map(|user| {
            let user_line = format!("user {} on {}", user.username, user.rules.join(" "));
            RespValue::BulkString(user_line.into())
        })
        .collect();
    Ok((RespValue::Array(list), WriteOutcome::DidNotWrite))
}

async fn handle_save(
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let server_config = ctx.state.config.lock().await;

    let Some(acl_file_path) = server_config.acl_file.as_ref() else {
        return Err(SpinelDBError::InvalidState(
            "ERR ACL file not configured. Set 'acl_file' in config.toml to use ACL SAVE."
                .to_string(),
        ));
    };
    let acl_file_path_clone = acl_file_path.clone();
    drop(server_config);

    let acl_config_arc = ctx.state.acl_config.read().await;

    let users_to_save = AclUsersFile {
        users: acl_config_arc.users.clone(),
    };

    let save_result = tokio::task::spawn_blocking(move || -> Result<(), SpinelDBError> {
        let json_string = serde_json::to_string_pretty(&users_to_save).map_err(|e| {
            SpinelDBError::Internal(format!("Failed to serialize ACL users to JSON: {e}"))
        })?;

        let temp_path_str = format!(
            "{}.tmp-acl-save-{}",
            acl_file_path_clone,
            rand::random::<u32>()
        );
        let temp_path = std::path::Path::new(&temp_path_str);

        std::fs::write(temp_path, json_string)
            .map_err(|e| SpinelDBError::Internal(format!("Failed to write temp ACL file: {e}")))?;

        std::fs::rename(temp_path, &acl_file_path_clone).map_err(|e| {
            SpinelDBError::Internal(format!("Failed to atomically rename ACL file: {e}"))
        })?;

        info!(
            "ACL user data saved successfully to '{}'",
            acl_file_path_clone
        );
        Ok(())
    })
    .await;

    match save_result {
        Ok(Ok(_)) => Ok((
            RespValue::SimpleString("OK".into()),
            WriteOutcome::DidNotWrite,
        )),
        Ok(Err(e)) => Err(e),
        Err(join_err) => Err(SpinelDBError::Internal(format!(
            "ACL SAVE task panicked: {join_err}"
        ))),
    }
}
