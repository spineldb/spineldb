// benches/command_bench.rs

//! Command execution benchmarks
//!
//! Measures the performance of various SpinelDB commands under different
//! workloads and data sizes.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use spineldb::config::Config;
use spineldb::core::commands::command_trait::CommandExt;
use spineldb::core::database::context::ExecutionContext;
use spineldb::core::database::core::Db;
use spineldb::core::protocol::RespFrame;
use spineldb::core::state::ServerState;
use spineldb::core::{Command, RespValue, SpinelDBError};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, reload};

/// TestContext provides a complete test environment with a real database instance
#[derive(Clone)]
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
    /// Handles transaction queuing when in a transaction
    pub async fn execute(&self, command: Command) -> Result<RespValue, SpinelDBError> {
        let session_id = 1; // Use a fixed session ID for tests

        // Handle transaction control commands first
        match command {
            Command::Multi => {
                use spineldb::core::handler::transaction_handler::TransactionHandler;
                let handler =
                    TransactionHandler::new(self.state.clone(), &self.db, session_id, None);
                return handler.handle_multi();
            }
            Command::Exec => {
                use spineldb::core::handler::transaction_handler::TransactionHandler;
                let mut handler =
                    TransactionHandler::new(self.state.clone(), &self.db, session_id, None);
                return handler.handle_exec().await;
            }
            Command::Discard => {
                use spineldb::core::handler::transaction_handler::TransactionHandler;
                let handler =
                    TransactionHandler::new(self.state.clone(), &self.db, session_id, None);
                return handler.handle_discard();
            }
            Command::Watch(cmd) => {
                use spineldb::core::handler::transaction_handler::TransactionHandler;
                let handler =
                    TransactionHandler::new(self.state.clone(), &self.db, session_id, None);
                return handler.handle_watch(cmd.keys).await;
            }
            Command::Unwatch(_) => {
                if let Some(mut tx_state) = self.db.tx_states.get_mut(&session_id) {
                    tx_state.watched_keys.clear();
                }
                return Ok(RespValue::SimpleString("OK".into()));
            }
            _ => {}
        }

        // For regular commands, execute through the database
        let locks = self.db.determine_locks_for_command(&command).await;

        let mut ctx = ExecutionContext {
            state: self.state.clone(),
            locks,
            db: &self.db,
            command: Some(command.clone()),
            session_id,
            authenticated_user: None,
        };

        let (resp, _outcome) = command.execute(&mut ctx).await?;
        Ok(resp)
    }

    // ===== String Command Helpers =====

    /// Helper to execute SET command
    pub async fn set(&self, key: &str, value: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"SET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(value.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute GET command
    pub async fn get(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"GET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute XLEN command
    pub async fn xlen(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"XLEN")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }

    // ===== Hash Command Helpers =====

    /// Helper to execute HSET command
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
    pub async fn hget(&self, key: &str, field: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"HGET")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(field.to_string())),
        ]))?;
        self.execute(command).await
    }

    /// Helper to execute LPOP command
    pub async fn lpop(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"LPOP")),
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

    // ===== List Command Helpers =====

    /// Helper to execute LPUSH command
    pub async fn lpush(&self, key: &str, values: &[&str]) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"LPUSH"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));
        for value in values {
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute LRANGE command
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

    // ===== Sorted Set Command Helpers =====

    /// Helper to execute ZADD command
    pub async fn zadd(
        &self,
        key: &str,
        members: &[(&str, &str)],
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

    /// Helper to execute ZRANGE command
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

    /// Helper to execute ZSCORE command
    pub async fn zscore(&self, key: &str, member: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"ZSCORE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(member.to_string())),
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

    /// Helper to execute DBSIZE command
    pub async fn dbsize(&self) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![RespFrame::BulkString(
            Bytes::from_static(b"DBSIZE"),
        )]))?;
        self.execute(command).await
    }

    /// Helper to execute FLUSHDB command (clears the current database)
    pub async fn flushdb(&self) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![RespFrame::BulkString(
            Bytes::from_static(b"FLUSHDB"),
        )]))?;
        self.execute(command).await
    }

    /// Helper to execute MULTI command
    pub async fn multi(&self) -> Result<RespValue, SpinelDBError> {
        let session_id = 1;
        use spineldb::core::handler::transaction_handler::TransactionHandler;
        TransactionHandler::new(self.state.clone(), &self.db, session_id, None).handle_multi()
    }

    /// Helper to execute EXEC command
    pub async fn exec(&self) -> Result<RespValue, SpinelDBError> {
        let session_id = 1;
        use spineldb::core::handler::transaction_handler::TransactionHandler;
        let mut handler = TransactionHandler::new(self.state.clone(), &self.db, session_id, None);
        handler.handle_exec().await
    }

    /// Helper to execute WATCH command
    pub async fn watch(&self, keys: &[&str]) -> Result<RespValue, SpinelDBError> {
        let session_id = 1;
        let keys_bytes: Vec<Bytes> = keys.iter().map(|k| Bytes::from(k.to_string())).collect();
        use spineldb::core::handler::transaction_handler::TransactionHandler;
        let handler = TransactionHandler::new(self.state.clone(), &self.db, session_id, None);
        handler.handle_watch(keys_bytes).await
    }

    // ===== Stream Command Helpers =====

    /// Helper to execute XADD command
    pub async fn xadd(
        &self,
        key: &str,
        fields: &[(&str, &str)],
        id: Option<&str>,
        maxlen: Option<(bool, usize)>,
        nomkstream: bool,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![RespFrame::BulkString(Bytes::from_static(b"XADD"))];
        frames.push(RespFrame::BulkString(Bytes::from(key.to_string())));

        if let Some((approx, count)) = maxlen {
            if approx {
                frames.push(RespFrame::BulkString(Bytes::from_static(b"MAXLEN")));
                frames.push(RespFrame::BulkString(Bytes::from_static(b"~")));
            } else {
                frames.push(RespFrame::BulkString(Bytes::from_static(b"MAXLEN")));
            }
            frames.push(RespFrame::BulkString(Bytes::from(count.to_string())));
        }

        if nomkstream {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"NOMKSTREAM")));
        }

        if let Some(id_str) = id {
            frames.push(RespFrame::BulkString(Bytes::from(id_str.to_string())));
        } else {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"*")));
        }

        for (field, value) in fields {
            frames.push(RespFrame::BulkString(Bytes::from(field.to_string())));
            frames.push(RespFrame::BulkString(Bytes::from(value.to_string())));
        }

        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }

    /// Helper to execute XRANGE command
    pub async fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Result<RespValue, SpinelDBError> {
        let mut frames = vec![
            RespFrame::BulkString(Bytes::from_static(b"XRANGE")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
            RespFrame::BulkString(Bytes::from(start.to_string())),
            RespFrame::BulkString(Bytes::from(end.to_string())),
        ];
        if let Some(c) = count {
            frames.push(RespFrame::BulkString(Bytes::from_static(b"COUNT")));
            frames.push(RespFrame::BulkString(Bytes::from(c.to_string())));
        }
        let command = Command::try_from(RespFrame::Array(frames))?;
        self.execute(command).await
    }
}

/// Benchmark basic string operations
pub fn bench_string_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("string_operations");

    group.bench_function("set_get_small", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for i in 0..iters {
                    let key = format!("key{}", i);
                    let value = format!("value{}", i);

                    ctx.set(&key, &value).await.unwrap();
                    let _ = ctx.get(&key).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("set_get_large", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for i in 0..iters {
                    let key = format!("key{}", i);
                    let value = "x".repeat(1024); // 1KB value

                    ctx.set(&key, &value).await.unwrap();
                    let _ = ctx.get(&key).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("incr_operations", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                ctx.set("counter", "0").await.unwrap();
                let start = std::time::Instant::now();

                for _ in 0..iters {
                    let _ = ctx.incr("counter").await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark hash operations
pub fn bench_hash_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("hash_operations");

    group.bench_function("hset_hget_small", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for i in 0..iters {
                    let key = "myhash";
                    let field = format!("field{}", i);
                    let value = format!("value{}", i);

                    ctx.hset(key, &[(&field, &value)]).await.unwrap();
                    let _ = ctx.hget(key, &field).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("hset_hget_large_hash", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                // Pre-populate hash with many fields
                for i in 0..100 {
                    ctx.hset(
                        "largehash",
                        &[(&format!("field{}", i), &format!("value{}", i))],
                    )
                    .await
                    .unwrap();
                }

                for i in 0..iters {
                    let field = format!("field{}", i % 100);
                    let value = format!("newvalue{}", i);

                    ctx.hset("largehash", &[(&field, &value)]).await.unwrap();
                    let _ = ctx.hget("largehash", &field).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark list operations
pub fn bench_list_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("list_operations");

    group.bench_function("lpush_lpop", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for i in 0..iters {
                    let value = format!("value{}", i);
                    ctx.lpush("mylist", &[&value]).await.unwrap();
                    let _ = ctx.lpop("mylist").await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("lpush_lrange", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                // Pre-populate list
                for i in 0..100 {
                    ctx.lpush("mylist", &[&format!("value{}", i)])
                        .await
                        .unwrap();
                }

                for _ in 0..iters {
                    let _ = ctx.lrange("mylist", 0, 99).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark set operations
pub fn bench_set_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("set_operations");

    group.bench_function("sadd_sismember", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for i in 0..iters {
                    let member = format!("member{}", i);
                    ctx.sadd("myset", &[&member]).await.unwrap();
                    let _ = ctx.sismember("myset", &member).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("sadd_smembers_large", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                // Pre-populate set with many members
                for i in 0..1000 {
                    ctx.sadd("largeset", &[&format!("member{}", i)])
                        .await
                        .unwrap();
                }

                for _ in 0..iters {
                    let _ = ctx.smembers("largeset").await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark sorted set operations
pub fn bench_sorted_set_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("sorted_set_operations");

    group.bench_function("zadd_zscore", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for i in 0..iters {
                    let member = format!("member{}", i);
                    ctx.zadd("myzset", &[(&format!("{}", i), &member)], &[])
                        .await
                        .unwrap();
                    let _ = ctx.zscore("myzset", &member).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("zadd_zrange_large", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                // Pre-populate sorted set
                for i in 0..1000 {
                    ctx.zadd(
                        "largezset",
                        &[(&format!("{}", i), &format!("member{}", i))],
                        &[],
                    )
                    .await
                    .unwrap();
                }

                for _ in 0..iters {
                    let _ = ctx.zrange("largezset", 0, 99, false).await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark transaction operations
pub fn bench_transaction_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("transaction_operations");

    group.bench_function("multi_exec_simple", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for _ in 0..iters {
                    ctx.multi().await.unwrap();
                    ctx.set("key1", "value1").await.unwrap();
                    ctx.set("key2", "value2").await.unwrap();
                    ctx.exec().await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("multi_exec_with_watch", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();

                for _ in 0..iters {
                    ctx.watch(&["key1"]).await.unwrap();
                    ctx.multi().await.unwrap();
                    ctx.set("key1", "value1").await.unwrap();
                    ctx.set("key2", "value2").await.unwrap();
                    ctx.exec().await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_string_operations,
    bench_hash_operations,
    bench_list_operations,
    bench_set_operations,
    bench_sorted_set_operations,
    bench_transaction_operations
);
criterion_main!(benches);
