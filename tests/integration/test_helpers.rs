// tests/integration/test_helpers.rs

//! Test helpers and utilities for integration tests

use bytes::Bytes;
use spineldb::config::Config;
use spineldb::core::Command;
use spineldb::core::RespValue;
use spineldb::core::SpinelDBError;
use spineldb::core::commands::command_trait::CommandExt;
use spineldb::core::database::context::ExecutionContext;
use spineldb::core::database::core::Db;
use spineldb::core::protocol::RespFrame;
use spineldb::core::state::ServerState;
use std::sync::Arc;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, reload};

/// TestContext provides a complete test environment with a real database instance
pub struct TestContext {
    pub state: Arc<ServerState>,
    pub db: Arc<Db>,
    pub db_index: usize,
}

impl TestContext {
    /// Creates a new test context with default configuration
    pub async fn new() -> Self {
        // Create a minimal config for testing with fewer databases to avoid stack overflow
        let mut config = Config::default();
        config.databases = 1; // Only 1 database for tests
        config.persistence.aof_enabled = false; // Disable AOF for tests
        config.persistence.spldb_enabled = false; // Disable SPLDB for tests
        Self::with_config(config).await
    }

    /// Creates a new test context with custom configuration
    pub async fn with_config(config: Config) -> Self {
        // Set up minimal tracing for tests
        let env_filter = EnvFilter::new("warn");
        let (filter, reload_handle) = reload::Layer::new(env_filter);

        // Initialize tracing (ignore error if already initialized)
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_test_writer())
            .try_init();

        let reload_handle = Arc::new(reload_handle);

        // Initialize server state
        let server_init = ServerState::initialize(config, reload_handle)
            .expect("Failed to initialize server state");

        let state = server_init.state;
        let db = state.get_db(0).expect("Failed to get database 0");

        Self {
            state,
            db,
            db_index: 0,
        }
    }

    /// Executes a command and returns the response value
    pub async fn execute(&self, command: Command) -> Result<RespValue, SpinelDBError> {
        let locks = self.db.determine_locks_for_command(&command).await;

        let mut ctx = ExecutionContext {
            state: self.state.clone(),
            locks,
            db: &self.db,
            command: Some(command.clone()),
            session_id: 1, // Use a fixed session ID for tests
            authenticated_user: None,
        };

        let (resp, _outcome) = command.execute(&mut ctx).await?;
        Ok(resp)
    }

    /// Executes multiple commands sequentially
    #[allow(dead_code)]
    pub async fn execute_multiple(
        &self,
        commands: Vec<Command>,
    ) -> Vec<Result<RespValue, SpinelDBError>> {
        let mut results = Vec::new();
        for cmd in commands {
            results.push(self.execute(cmd).await);
        }
        results
    }

    /// Helper to execute a SET command
    pub async fn set(&self, key: &str, value: &str) -> Result<RespValue, SpinelDBError> {
        let args = vec![
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ];
        let command = Command::try_from(RespFrame::Array(
            vec![RespFrame::BulkString(Bytes::from_static(b"SET"))]
                .into_iter()
                .chain(args)
                .collect(),
        ))?;
        self.execute(command).await
    }

    /// Helper to execute a GET command
    pub async fn get(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"GET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute a DEL command
    pub async fn del(&self, keys: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"DEL"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute an EXISTS command
    pub async fn exists(&self, keys: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"EXISTS"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute a FLUSHDB command (clears the current database)
    #[allow(dead_code)]
    pub async fn flushdb(&self) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![RespFrame::BulkString(
            Bytes::from_static(b"FLUSHDB"),
        )]))?;
        self.execute(command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_context_creation() {
        let ctx = TestContext::new().await;
        assert_eq!(ctx.db_index, 0);
    }

    #[tokio::test]
    async fn test_set_get_helper() {
        let ctx = TestContext::new().await;

        // SET should return OK
        let result = ctx.set("test_key", "test_value").await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".into()));

        // GET should return the value
        let result = ctx.get("test_key").await.unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("test_value")));
    }
}
