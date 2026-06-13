// tests/common/mod.rs

//! Shared test utilities for integration tests.
//!
//! Each test should call [`start_server`] to spin up a fresh SpinelDB instance
//! on a random local port, then use [`Client`] to send RESP commands and
//! read responses.
//!
//! The test framework uses Tokio's `#[tokio::test]` runtime. Tracing is
//! initialized exactly once via `try_init` so concurrent tests don't panic
//! when re-initializing the global subscriber.

#![allow(dead_code)]

use spineldb::config::{
    AppendFsync, CacheConfig, Config, PersistenceConfig, ReplicationConfig,
    ReplicationPrimaryConfig,
};
use spineldb::core::RespValue;
use spineldb::core::protocol::{RespFrame, RespFrameCodec};
use spineldb::server;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_util::codec::Decoder;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

/// Initialize the tracing subscriber exactly once for the whole test binary.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("warn"))
        .with_test_writer()
        .try_init();
}

/// Find a free TCP port by binding to 0 and reading the OS-assigned port.
pub async fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Build a minimal in-memory [`Config`] that uses the given temp directory
/// and a caller-chosen port. Persistence and clustering are disabled.
pub fn make_config(temp_dir: &TempDir, port: u16) -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port,
        password: None,
        log_level: "warn".to_string(),
        max_clients: 16,
        maxmemory: Some(64 * 1024 * 1024),
        maxmemory_policy: Default::default(),
        persistence: PersistenceConfig {
            aof_enabled: false,
            aof_path: temp_dir
                .path()
                .join("spineldb.aof")
                .to_string_lossy()
                .to_string(),
            appendfsync: AppendFsync::EverySec,
            auto_aof_rewrite_percentage: 100,
            auto_aof_rewrite_min_size: 64 * 1024 * 1024,
            aof_rewrite_buffer_limit: 64 * 1024 * 1024,
            aof_channel_capacity: 0,
            aof_enqueue_timeout_ms: 0,
            spldb_enabled: false,
            spldb_path: temp_dir
                .path()
                .join("dump.spldb")
                .to_string_lossy()
                .to_string(),
            save_rules: vec![],
        },
        replication: ReplicationConfig::Primary(ReplicationPrimaryConfig::default()),
        databases: 16,
        cluster: Default::default(),
        tls: Default::default(),
        safety: Default::default(),
        security: Default::default(),
        acl_file: None,
        acl: Default::default(),
        cache: CacheConfig {
            on_disk_path: temp_dir
                .path()
                .join("cache_files")
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        },
        metrics: Default::default(),
    }
}

/// Wait until the configured port accepts TCP connections, or fail.
pub async fn wait_for_server(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("server did not become ready at {addr} within 10s");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Convert a [`RespFrame`] into a [`RespValue`] for ergonomic comparison.
pub fn frame_to_value(frame: RespFrame) -> RespValue {
    match frame {
        RespFrame::SimpleString(s) => RespValue::SimpleString(s),
        RespFrame::BulkString(b) => RespValue::BulkString(b),
        RespFrame::Integer(i) => RespValue::Integer(i),
        RespFrame::Array(arr) => RespValue::Array(arr.into_iter().map(frame_to_value).collect()),
        RespFrame::Null => RespValue::Null,
        RespFrame::NullArray => RespValue::NullArray,
        RespFrame::Error(s) => RespValue::Error(s),
        RespFrame::Boolean(b) => RespValue::Boolean(b),
        RespFrame::Double(d) => RespValue::Double(d),
        RespFrame::BigNumber(s) => RespValue::BigNumber(s),
        RespFrame::Map(m) => RespValue::Map(
            m.into_iter()
                .map(|(k, v)| (frame_to_value(k), frame_to_value(v)))
                .collect(),
        ),
        RespFrame::Set(s) => RespValue::Set(s.into_iter().map(frame_to_value).collect()),
        RespFrame::Push(p) => RespValue::Push(p.into_iter().map(frame_to_value).collect()),
        RespFrame::VerbatimString(fmt, data) => RespValue::VerbatimString(fmt, data),
        RespFrame::Attribute(attr, inner) => RespValue::Attribute(
            attr.into_iter()
                .map(|(k, v)| (frame_to_value(k), frame_to_value(v)))
                .collect(),
            Box::new(frame_to_value(*inner)),
        ),
    }
}

/// Holds the running server task handle and the temp directory.
pub struct ServerHandle {
    pub port: u16,
    pub temp_dir: TempDir,
    pub join: tokio::task::JoinHandle<anyhow::Result<()>>,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ServerHandle {
    /// Send a shutdown signal and abort the server task. Idempotent.
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.join.abort();
    }
}

/// Spin up a fresh SpinelDB server with a password set.
pub async fn start_server_with_password(password: &str) -> ServerHandle {
    init_tracing();
    let temp_dir = TempDir::new().expect("create tempdir");
    let port = pick_free_port().await;
    let mut config = make_config(&temp_dir, port);
    config.password = Some(password.to_string());

    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(EnvFilter::new("warn"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();
    let reload_handle = Arc::new(reload_handle);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        tokio::select! {
            res = server::run(config, reload_handle) => res,
            _ = shutdown_rx => Ok(()),
        }
    });

    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    wait_for_server(addr).await;

    ServerHandle {
        port,
        temp_dir,
        join,
        shutdown_tx: Some(shutdown_tx),
    }
}

/// Spin up a fresh SpinelDB server on a random local port.
pub async fn start_server() -> ServerHandle {
    init_tracing();
    let temp_dir = TempDir::new().expect("create tempdir");
    let port = pick_free_port().await;
    let config = make_config(&temp_dir, port);

    // Build a reload handle — server::run requires one.
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(EnvFilter::new("warn"));
    // Initialize the global subscriber exactly once; subsequent calls
    // are silently ignored.
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();
    let reload_handle = Arc::new(reload_handle);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        // Race the server against the shutdown signal; whichever fires
        // first wins. The server runs forever under normal operation.
        tokio::select! {
            res = server::run(config, reload_handle) => res,
            _ = shutdown_rx => Ok(()),
        }
    });

    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    wait_for_server(addr).await;

    ServerHandle {
        port,
        temp_dir,
        join,
        shutdown_tx: Some(shutdown_tx),
    }
}

/// Encode a command as a RESP array of bulk strings.
pub fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// A thin client for sending RESP commands to a running server.
pub struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    /// Connect to the server at the given address.
    pub async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    /// Send a RESP-encoded command.
    pub async fn send(&mut self, parts: &[&[u8]]) {
        self.stream
            .write_all(&encode_command(parts))
            .await
            .expect("write");
        self.stream.flush().await.expect("flush");
    }

    /// Read a single raw `RespFrame` from the stream.
    pub async fn read_frame(&mut self) -> Result<RespFrame, std::io::Error> {
        loop {
            let mut tmp_buf: bytes::BytesMut = (&self.buf[..]).into();
            match RespFrameCodec.decode(&mut tmp_buf) {
                Ok(Some(frame)) => {
                    let consumed = self.buf.len() - tmp_buf.len();
                    self.buf.drain(..consumed);
                    return Ok(frame);
                }
                Ok(None) => {
                    // Need more data.
                }
                Err(e) => {
                    return Err(std::io::Error::other(format!("codec error: {e}")));
                }
            }
            // Read more from the stream.
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed connection",
                ));
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Read a single RESP response and convert to RespValue.
    pub async fn read_response(&mut self) -> RespValue {
        let frame = self.read_frame().await.expect("read frame");
        frame_to_value(frame)
    }

    /// Send a command and read a single response.
    pub async fn cmd(&mut self, parts: &[&[u8]]) -> RespValue {
        self.send(parts).await;
        self.read_response().await
    }
}
