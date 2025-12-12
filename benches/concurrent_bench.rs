// tests/integration/performance/concurrent_bench.rs

//! Concurrent access benchmarks
//!
//! Measures the performance of SpinelDB under concurrent workloads,
//! testing locking, contention, and scalability characteristics.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use spineldb::config::Config;
use spineldb::core::commands::command_trait::CommandExt;
use spineldb::core::database::context::ExecutionContext;
use spineldb::core::database::core::Db;
use spineldb::core::protocol::RespFrame;
use spineldb::core::state::ServerState;
use spineldb::core::{Command, RespValue, SpinelDBError};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task;
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

    /// Helper to execute INCR command
    pub async fn incr(&self, key: &str) -> Result<RespValue, SpinelDBError> {
        let command = Command::try_from(RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"INCR")),
            RespFrame::BulkString(Bytes::from(key.to_string())),
        ]))?;
        self.execute(command).await
    }
}

/// Benchmark concurrent read operations
pub fn bench_concurrent_reads(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_reads");

    group.bench_function("10_concurrent_gets", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;

                // Pre-populate with data
                for i in 0..100 {
                    ctx.set(&format!("key{}", i), &format!("value{}", i))
                        .await
                        .unwrap();
                }

                let start = std::time::Instant::now();
                let mut handles = vec![];

                for _ in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        for i in 0..10 {
                            let key = format!("key{}", i % 100);
                            let _ = black_box(ctx_clone.get(&key).await.unwrap());
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("100_concurrent_gets", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;

                // Pre-populate with data
                for i in 0..100 {
                    ctx.set(&format!("key{}", i), &format!("value{}", i))
                        .await
                        .unwrap();
                }

                let start = std::time::Instant::now();
                let mut handles = vec![];

                for _ in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        for i in 0..100 {
                            let key = format!("key{}", i % 100);
                            let _ = black_box(ctx_clone.get(&key).await.unwrap());
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark concurrent write operations
pub fn bench_concurrent_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_writes");

    group.bench_function("10_concurrent_sets", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();
                let mut handles = vec![];

                for i in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        for j in 0..10 {
                            let key = format!("key{}_{}", i, j);
                            let value = format!("value{}_{}", i, j);
                            ctx_clone.set(&key, &value).await.unwrap();
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("same_key_concurrent_incr", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                ctx.set("counter", "0").await.unwrap();

                let start = std::time::Instant::now();
                let mut handles = vec![];

                for _ in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        let _ = black_box(ctx_clone.incr("counter").await.unwrap());
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark mixed read/write workloads
pub fn bench_mixed_workloads(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("mixed_workloads");

    group.bench_function("read_heavy_workload", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;

                // Pre-populate with data
                for i in 0..100 {
                    ctx.set(&format!("key{}", i), &format!("value{}", i))
                        .await
                        .unwrap();
                }

                let start = std::time::Instant::now();
                let mut handles = vec![];

                for _ in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        // 80% reads, 20% writes
                        for i in 0..100 {
                            if i % 5 == 0 {
                                let key = format!("write_key{}", i);
                                let value = format!("write_value{}", i);
                                let _ = black_box(ctx_clone.set(&key, &value).await.unwrap());
                            } else {
                                let key = format!("key{}", i % 100);
                                let _ = black_box(ctx_clone.get(&key).await.unwrap());
                            }
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("write_heavy_workload", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;

                let start = std::time::Instant::now();
                let mut handles = vec![];

                for _ in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        // 20% reads, 80% writes
                        for i in 0..100 {
                            if i % 5 == 0 {
                                let key = format!("read_key{}", i % 10);
                                let _ =
                                    black_box(ctx_clone.get(&key).await.unwrap_or(RespValue::Null));
                            } else {
                                let key = format!("write_key{}", i);
                                let value = format!("write_value{}", i);
                                let _ = black_box(ctx_clone.set(&key, &value).await.unwrap());
                            }
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark hash operations under concurrency
pub fn bench_concurrent_hash_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_hash_operations");

    group.bench_function("concurrent_hash_sets", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let start = std::time::Instant::now();
                let mut handles = vec![];

                for i in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        for j in 0..10 {
                            let field = format!("field{}_{}", i, j);
                            let value = format!("value{}_{}", i, j);
                            ctx_clone.hset("myhash", &[(&field, &value)]).await.unwrap();
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("concurrent_hash_gets", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let ctx = TestContext::new().await;

                // Pre-populate hash
                for i in 0..100 {
                    ctx.hset(
                        "myhash",
                        &[(&format!("field{}", i), &format!("value{}", i))],
                    )
                    .await
                    .unwrap();
                }

                let start = std::time::Instant::now();
                let mut handles = vec![];

                for _ in 0..iters {
                    let ctx_clone = ctx.clone();
                    let handle = task::spawn(async move {
                        for i in 0..10 {
                            let field = format!("field{}", i % 100);
                            let _ = black_box(ctx_clone.hget("myhash", &field).await.unwrap());
                        }
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

/// Benchmark transaction concurrency
pub fn bench_transaction_concurrency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("transaction_concurrency");

    group.bench_function("concurrent_transactions", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = std::time::Instant::now();
                let mut handles = vec![];

                for i in 0..iters {
                    let handle = task::spawn(async move {
                        let ctx = TestContext::new().await;
                        ctx.multi().await.unwrap();
                        ctx.set(&format!("key{}_1", i), &format!("value{}_1", i))
                            .await
                            .unwrap();
                        ctx.set(&format!("key{}_2", i), &format!("value{}_2", i))
                            .await
                            .unwrap();
                        ctx.exec().await.unwrap();
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("transaction_with_watch", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = std::time::Instant::now();
                let mut handles = vec![];

                for i in 0..iters {
                    let handle = task::spawn(async move {
                        let ctx = TestContext::new().await;
                        // Use different keys to avoid watch conflicts
                        let key = format!("watch_key{}", i);
                        ctx.set(&key, "initial").await.unwrap();
                        ctx.watch(&[&key]).await.unwrap();

                        ctx.multi().await.unwrap();
                        ctx.set(&key, &format!("value{}", i)).await.unwrap();
                        ctx.exec().await.unwrap();
                    });
                    handles.push(handle);
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_reads,
    bench_concurrent_writes,
    bench_mixed_workloads,
    bench_concurrent_hash_operations,
    bench_transaction_concurrency
);
criterion_main!(benches);
