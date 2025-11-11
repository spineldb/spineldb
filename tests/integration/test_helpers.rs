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

    /// Executes a command from a RespFrame
    #[allow(dead_code)]
    pub async fn execute_frame(&self, frame: RespFrame) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(frame)?;
        self.execute(command).await
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

    /// Helper to execute SET with NX option
    #[allow(dead_code)] // Available for tests that need SET NX
    pub async fn set_nx(&self, key: &str, value: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
            RespFrame::BulkString(Bytes::from_static(b"NX")),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute SET with XX option
    #[allow(dead_code)] // Available for tests that need SET XX
    pub async fn set_xx(&self, key: &str, value: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
            RespFrame::BulkString(Bytes::from_static(b"XX")),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute APPEND command
    pub async fn append(&self, key: &str, value: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"APPEND")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute STRLEN command
    pub async fn strlen(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"STRLEN")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute INCR command
    pub async fn incr(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"INCR")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute DECR command
    pub async fn decr(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"DECR")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute INCRBY command
    pub async fn incrby(&self, key: &str, increment: i64) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"INCRBY")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(increment.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute DECRBY command
    pub async fn decrby(&self, key: &str, decrement: i64) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"DECRBY")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(decrement.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to create a list for type error tests
    pub async fn create_list(&self, key: &str, value: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LPUSH")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to create a hash for type error tests
    pub async fn create_hash(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HSET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to create a set for type error tests
    pub async fn create_set(&self, key: &str, member: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SADD")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
        ]))?;
        self.execute(command).await
    }

    // ===== Set Command Helpers =====

    /// Helper to execute SADD command
    pub async fn sadd(&self, key: &str, members: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SADD"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for member in members {
            frames.push(RespFrame::BulkString(Bytes::from(member.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SMEMBERS command
    pub async fn smembers(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SMEMBERS")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute SCARD command
    pub async fn scard(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SCARD")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute SISMEMBER command
    pub async fn sismember(&self, key: &str, member: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SISMEMBER")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute SREM command
    pub async fn srem(&self, key: &str, members: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SREM"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for member in members {
            frames.push(RespFrame::BulkString(Bytes::from(member.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SPOP command
    pub async fn spop(&self, key: &str, count: Option<usize>) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"SPOP")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ];
        if let Some(c) = count {
            frames.push(RespFrame::BulkString(Bytes::from(c.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SRANDMEMBER command
    pub async fn srandmember(
        &self,
        key: &str,
        count: Option<i64>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"SRANDMEMBER")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ];
        if let Some(c) = count {
            frames.push(RespFrame::BulkString(Bytes::from(c.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SMOVE command
    pub async fn smove(
        &self,
        source: &str,
        destination: &str,
        member: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SMOVE")),
            RespFrame::BulkString(Bytes::from(source.to_string())),
            RespFrame::BulkString(Bytes::from(destination.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute SMISMEMBER command
    pub async fn smismember(
        &self,
        key: &str,
        members: &[&str],
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SMISMEMBER"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for member in members {
            frames.push(RespFrame::BulkString(Bytes::from(member.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SINTER command
    pub async fn sinter(&self, keys: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SINTER"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SUNION command
    pub async fn sunion(&self, keys: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SUNION"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SDIFF command
    pub async fn sdiff(&self, keys: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SDIFF"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SINTERSTORE command
    pub async fn sinterstore(
        &self,
        destination: &str,
        keys: &[&str],
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SINTERSTORE"))];
        frames.push(RespFrame::BulkString(Bytes::from(destination.to_string())));
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SUNIONSTORE command
    pub async fn sunionstore(
        &self,
        destination: &str,
        keys: &[&str],
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SUNIONSTORE"))];
        frames.push(RespFrame::BulkString(Bytes::from(destination.to_string())));
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute SDIFFSTORE command
    pub async fn sdiffstore(
        &self,
        destination: &str,
        keys: &[&str],
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"SDIFFSTORE"))];
        frames.push(RespFrame::BulkString(Bytes::from(destination.to_string())));
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute TTL command
    pub async fn ttl(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"TTL")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute GETRANGE command
    pub async fn getrange(
        &self,
        key: &str,
        start: i64,
        end: i64,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"GETRANGE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(end.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute SETRANGE command
    pub async fn setrange(
        &self,
        key: &str,
        offset: i64,
        value: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SETRANGE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(offset.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute GETBIT command
    #[allow(dead_code)] // Available for tests that need GETBIT
    pub async fn getbit(&self, key: &str, offset: i64) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"GETBIT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(offset.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute SETBIT command
    #[allow(dead_code)] // Available for tests that need SETBIT
    pub async fn setbit(
        &self,
        key: &str,
        offset: i64,
        value: u8,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SETBIT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(offset.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute BITCOUNT command
    #[allow(dead_code)] // Available for tests that need BITCOUNT
    pub async fn bitcount(
        &self,
        key: &str,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"BITCOUNT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ];
        if let (Some(s), Some(e)) = (start, end) {
            frames.push(RespFrame::BulkString(Bytes::from(s.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(e.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute BITPOS command
    #[allow(dead_code)] // Available for tests that need BITPOS
    pub async fn bitpos(
        &self,
        key: &str,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"BITPOS")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(bit.to_string())),
        ];
        if let (Some(s), Some(e)) = (start, end) {
            frames.push(RespFrame::BulkString(Bytes::from(s.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(e.to_string())));
        } else if let Some(s) = start {
            frames.push(RespFrame::BulkString(Bytes::from(s.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute INCRBYFLOAT command
    #[allow(dead_code)] // Available for tests that need INCRBYFLOAT
    pub async fn incrbyfloat(&self, key: &str, increment: f64) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"INCRBYFLOAT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(increment.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute MGET command
    pub async fn mget(&self, keys: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"MGET"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute MSET command
    pub async fn mset(&self, key_values: &[(&str, &str)]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"MSET"))];
        for (key, value) in key_values {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute MSETNX command
    #[allow(dead_code)] // Available for tests that need MSETNX
    pub async fn msetnx(&self, key_values: &[(&str, &str)]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"MSETNX"))];
        for (key, value) in key_values {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    // ===== List Command Helpers =====

    /// Helper to execute LPUSH command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn lpush(&self, key: &str, values: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"LPUSH"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for value in values {
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute RPUSH command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn rpush(&self, key: &str, values: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"RPUSH"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for value in values {
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute LPOP command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn lpop(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LPOP")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute RPOP command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn rpop(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"RPOP")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LLEN command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn llen(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LLEN")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LRANGE command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn lrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LRANGE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(stop.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LINDEX command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn lindex(&self, key: &str, index: i64) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LINDEX")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(index.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LSET command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn lset(
        &self,
        key: &str,
        index: i64,
        value: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LSET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(index.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LTRIM command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn ltrim(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LTRIM")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(stop.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LINSERT command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn linsert(
        &self,
        key: &str,
        position: &str, // "BEFORE" or "AFTER"
        pivot: &str,
        value: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LINSERT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(position.to_string())),
            RespFrame::BulkString(Bytes::from(pivot.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LREM command
    #[allow(dead_code)] // Used in list_commands_test.rs
    pub async fn lrem(
        &self,
        key: &str,
        count: i64,
        value: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LREM")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(count.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LPUSHX command
    #[allow(dead_code)] // Available for tests that need LPUSHX
    pub async fn lpushx(&self, key: &str, values: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"LPUSHX"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for value in values {
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute RPUSHX command
    #[allow(dead_code)] // Available for tests that need RPUSHX
    pub async fn rpushx(&self, key: &str, values: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"RPUSHX"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for value in values {
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute LPOS command
    #[allow(dead_code)] // Available for tests that need LPOS
    pub async fn lpos(
        &self,
        key: &str,
        element: &str,
        rank: Option<i64>,
        count: Option<u64>,
        max_len: Option<u64>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"LPOS")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(element.to_string())),
        ];
        if let Some(r) = rank {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"RANK")));
            frames.push(RespFrame::BulkString(Bytes::from(r.to_string())));
        }
        if let Some(c) = count {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"COUNT")));
            frames.push(RespFrame::BulkString(Bytes::from(c.to_string())));
        }
        if let Some(m) = max_len {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"MAXLEN")));
            frames.push(RespFrame::BulkString(Bytes::from(m.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute LMOVE command
    #[allow(dead_code)] // Available for tests that need LMOVE
    pub async fn lmove(
        &self,
        source: &str,
        destination: &str,
        from: &str, // "LEFT" or "RIGHT"
        to: &str,   // "LEFT" or "RIGHT"
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LMOVE")),
            RespFrame::BulkString(Bytes::from(source.to_string())),
            RespFrame::BulkString(Bytes::from(destination.to_string())),
            RespFrame::BulkString(Bytes::from(from.to_string())),
            RespFrame::BulkString(Bytes::from(to.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute BLPOP command (non-blocking test scenarios)
    #[allow(dead_code)] // Available for tests that need BLPOP
    pub async fn blpop(&self, keys: &[&str], timeout: f64) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"BLPOP"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        frames.push(RespFrame::BulkString(Bytes::from(timeout.to_string())));
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute BRPOP command (non-blocking test scenarios)
    #[allow(dead_code)] // Available for tests that need BRPOP
    pub async fn brpop(&self, keys: &[&str], timeout: f64) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"BRPOP"))];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        frames.push(RespFrame::BulkString(Bytes::from(timeout.to_string())));
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute BLMOVE command (non-blocking test scenarios)
    #[allow(dead_code)] // Available for tests that need BLMOVE
    pub async fn blmove(
        &self,
        source: &str,
        destination: &str,
        from: &str,
        to: &str,
        timeout: f64,
    ) -> Result<RespValue, SpinelDBError> {
        let frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"BLMOVE")),
            RespFrame::BulkString(Bytes::from(source.to_string())),
            RespFrame::BulkString(Bytes::from(destination.to_string())),
            RespFrame::BulkString(Bytes::from(from.to_string())),
            RespFrame::BulkString(Bytes::from(to.to_string())),
            RespFrame::BulkString(Bytes::from(timeout.to_string())),
        ];
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    // ===== Hash Command Helpers =====

    /// Helper to execute HSET command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hset(
        &self,
        key: &str,
        field_values: &[(&str, &str)],
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"HSET"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for (field, value) in field_values {
            frames.push(RespFrame::BulkString(Bytes::from(field.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute HGET command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hget(&self, key: &str, field: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HGET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HDEL command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hdel(&self, key: &str, fields: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"HDEL"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for field in fields {
            frames.push(RespFrame::BulkString(Bytes::from(field.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute HGETALL command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hgetall(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HGETALL")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HMGET command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hmget(&self, key: &str, fields: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"HMGET"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for field in fields {
            frames.push(RespFrame::BulkString(Bytes::from(field.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute HEXISTS command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hexists(&self, key: &str, field: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HEXISTS")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HSETNX command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hsetnx(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HSETNX")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HLEN command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hlen(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HLEN")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HKEYS command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hkeys(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HKEYS")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HVALS command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hvals(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HVALS")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HINCRBY command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hincrby(
        &self,
        key: &str,
        field: &str,
        increment: i64,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HINCRBY")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
            RespFrame::BulkString(Bytes::from(increment.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HINCRBYFLOAT command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hincrbyfloat(
        &self,
        key: &str,
        field: &str,
        increment: f64,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HINCRBYFLOAT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
            RespFrame::BulkString(Bytes::from(increment.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HSTRLEN command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hstrlen(&self, key: &str, field: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HSTRLEN")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute HRANDFIELD command
    #[allow(dead_code)] // Used in hash_commands_test.rs
    pub async fn hrandfield(
        &self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"HRANDFIELD"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        if let Some(c) = count {
            frames.push(RespFrame::BulkString(Bytes::from(c.to_string())));
        }
        if with_values {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"WITHVALUES")));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    // ===== Sorted Set (ZSET) Commands =====

    /// Helper to execute ZADD command
    /// members: Vec of (score, member) tuples
    /// options: Vec of option strings like "NX", "XX", "CH", "INCR", "GT", "LT"
    pub async fn zadd(
        &self,
        key: &str,
        members: &[(&str, &str)], // (score, member)
        options: &[&str],
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"ZADD"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for option in options {
            frames.push(RespFrame::BulkString(Bytes::from(option.to_string())));
        }
        for (score, member) in members {
            frames.push(RespFrame::BulkString(Bytes::from(score.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(member.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZCARD command
    pub async fn zcard(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZCARD")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZSCORE command
    pub async fn zscore(&self, key: &str, member: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZSCORE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZRANK command
    pub async fn zrank(&self, key: &str, member: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZRANK")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZREVRANK command
    pub async fn zrevrank(&self, key: &str, member: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZREVRANK")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZCOUNT command
    pub async fn zcount(
        &self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZCOUNT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(min.to_string())),
            RespFrame::BulkString(Bytes::from(max.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZRANGE command
    /// with_scores: if true, includes WITHSCORES option
    pub async fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZRANGE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(stop.to_string())),
        ];
        if with_scores {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"WITHSCORES")));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZREVRANGE command
    pub async fn zrevrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZREVRANGE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(stop.to_string())),
        ];
        if with_scores {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"WITHSCORES")));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZREM command
    pub async fn zrem(&self, key: &str, members: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"ZREM"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for member in members {
            frames.push(RespFrame::BulkString(Bytes::from(member.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZINCRBY command
    pub async fn zincrby(
        &self,
        key: &str,
        increment: &str,
        member: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZINCRBY")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(increment.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZPOPMAX command
    pub async fn zpopmax(
        &self,
        key: &str,
        count: Option<usize>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZPOPMAX")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ];
        if let Some(c) = count {
            frames.push(RespFrame::BulkString(Bytes::from(c.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZPOPMIN command
    pub async fn zpopmin(
        &self,
        key: &str,
        count: Option<usize>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZPOPMIN")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ];
        if let Some(c) = count {
            frames.push(RespFrame::BulkString(Bytes::from(c.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZMSCORE command
    pub async fn zmscore(&self, key: &str, members: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"ZMSCORE"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for member in members {
            frames.push(RespFrame::BulkString(Bytes::from(member.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZRANGEBYSCORE command
    pub async fn zrangebyscore(
        &self,
        key: &str,
        min: &str,
        max: &str,
        with_scores: bool,
        limit: Option<(usize, usize)>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZRANGEBYSCORE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(min.to_string())),
            RespFrame::BulkString(Bytes::from(max.to_string())),
        ];
        if with_scores {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"WITHSCORES")));
        }
        if let Some((offset, count)) = limit {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"LIMIT")));
            frames.push(RespFrame::BulkString(Bytes::from(offset.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(count.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZREMRANGEBYRANK command
    pub async fn zremrangebyrank(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZREMRANGEBYRANK")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(stop.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZREMRANGEBYSCORE command
    pub async fn zremrangebyscore(
        &self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZREMRANGEBYSCORE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(min.to_string())),
            RespFrame::BulkString(Bytes::from(max.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZUNIONSTORE command
    pub async fn zunionstore(
        &self,
        destination: &str,
        keys: &[&str],
        weights: Option<&[&str]>,
        aggregate: Option<&str>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZUNIONSTORE")),
            RespFrame::BulkString(Bytes::from(destination.to_string())),
            RespFrame::BulkString(Bytes::from(keys.len().to_string())),
        ];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        if let Some(w) = weights {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"WEIGHTS")));
            for weight in w {
                frames.push(RespFrame::BulkString(Bytes::from(weight.to_string())));
            }
        }
        if let Some(agg) = aggregate {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"AGGREGATE")));
            frames.push(RespFrame::BulkString(Bytes::from(agg.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZINTERSTORE command
    pub async fn zinterstore(
        &self,
        destination: &str,
        keys: &[&str],
        weights: Option<&[&str]>,
        aggregate: Option<&str>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZINTERSTORE")),
            RespFrame::BulkString(Bytes::from(destination.to_string())),
            RespFrame::BulkString(Bytes::from(keys.len().to_string())),
        ];
        for key in keys {
            frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        }
        if let Some(w) = weights {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"WEIGHTS")));
            for weight in w {
                frames.push(RespFrame::BulkString(Bytes::from(weight.to_string())));
            }
        }
        if let Some(agg) = aggregate {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"AGGREGATE")));
            frames.push(RespFrame::BulkString(Bytes::from(agg.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZLEXCOUNT command
    pub async fn zlexcount(
        &self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZLEXCOUNT")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(min.to_string())),
            RespFrame::BulkString(Bytes::from(max.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZRANGEBYLEX command
    pub async fn zrangebylex(
        &self,
        key: &str,
        min: &str,
        max: &str,
        limit: Option<(usize, usize)>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZRANGEBYLEX")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(min.to_string())),
            RespFrame::BulkString(Bytes::from(max.to_string())),
        ];
        if let Some((offset, count)) = limit {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"LIMIT")));
            frames.push(RespFrame::BulkString(Bytes::from(offset.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(count.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute ZREMRANGEBYLEX command
    pub async fn zremrangebylex(
        &self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZREMRANGEBYLEX")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(min.to_string())),
            RespFrame::BulkString(Bytes::from(max.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute ZRANGESTORE command
    pub async fn zrangestore(
        &self,
        destination: &str,
        source: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
        rev: bool,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"ZRANGESTORE")),
            RespFrame::BulkString(Bytes::from(destination.to_string())),
            RespFrame::BulkString(Bytes::from(source.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(stop.to_string())),
        ];
        if with_scores {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"WITHSCORES")));
        }
        if rev {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"REV")));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }
}

// ===== Test Assertion Helpers =====

/// Helper to assert that a RespValue is an array with expected string values
///
/// **Usage:**
/// ```rust
/// let result = ctx.lrange("mylist", 0, -1).await.unwrap();
/// assert_lrange_equals(&result, &["value1", "value2", "value3"], "test description");
/// ```
pub fn assert_lrange_equals(result: &RespValue, expected: &[&'static str], message: &str) {
    match result {
        RespValue::Array(values) => {
            assert_eq!(
                values.len(),
                expected.len(),
                "{}: length mismatch, expected {}, got {}",
                message,
                expected.len(),
                values.len()
            );
            for (i, (actual, expected_str)) in values.iter().zip(expected.iter()).enumerate() {
                assert_eq!(
                    actual,
                    &RespValue::BulkString(Bytes::from(*expected_str)),
                    "{}: mismatch at index {}, expected '{}', got {:?}",
                    message,
                    i,
                    expected_str,
                    actual
                );
            }
        }
        _ => panic!("{}: Expected array response, got {:?}", message, result),
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
