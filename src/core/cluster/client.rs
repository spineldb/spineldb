// src/core/cluster/client.rs

//! Defines an internal client for cluster orchestration commands like RESHARD.

// Use the correct, refactored path for ClusterInfo and ClusterSubcommand.
use crate::core::Command;
use crate::core::commands::cluster::{ClusterInfo, ClusterSubcommand};
use crate::core::protocol::{RespFrame, RespFrameCodec};
use anyhow::{Result, anyhow};
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

// Timeout constants for robust network operations.
const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(3);

/// An internal client for sending commands to other nodes in the cluster.
pub struct ClusterClient {
    stream: TcpStream,
    codec: RespFrameCodec,
}

impl ClusterClient {
    /// Creates a TCP connection to the target node's address with a timeout.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream =
            tokio::time::timeout(CLIENT_CONNECT_TIMEOUT, TcpStream::connect(addr)).await??;
        Ok(Self {
            stream,
            codec: RespFrameCodec,
        })
    }

    /// A generic method to send a single command frame and receive a single reply frame.
    async fn send_and_receive(&mut self, frame: RespFrame) -> Result<RespFrame> {
        // 1. Encode the command into a byte buffer.
        let mut write_buf = BytesMut::new();
        self.codec.encode(frame, &mut write_buf)?;

        // 2. Send the buffer to the target server with a write timeout.
        let write_fut = self.stream.write_all(&write_buf);
        tokio::time::timeout(CLIENT_WRITE_TIMEOUT, write_fut)
            .await
            .map_err(|_| anyhow!("Write timeout while sending command"))??;

        // 3. Read the reply from the server in a loop.
        let mut read_buf = BytesMut::with_capacity(4096);
        loop {
            let read_fut = self.stream.read_buf(&mut read_buf);
            match tokio::time::timeout(CLIENT_READ_TIMEOUT, read_fut).await {
                Ok(Ok(0)) => return Err(anyhow!("Connection closed by peer")),
                Ok(Ok(_)) => {
                    // Attempt to decode a full frame from the buffer.
                    if let Some(reply) = self.codec.decode(&mut read_buf)? {
                        return Ok(reply);
                    }
                    // If data is not yet complete, the loop continues.
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => return Err(anyhow!("Read timeout while waiting for response")),
            }
        }
    }

    /// Sends a `CLUSTER SETSLOT ...` command and expects an "OK" reply.
    pub async fn cluster_setslot(&mut self, args: Vec<Bytes>) -> Result<()> {
        let cmd_parts: Vec<RespFrame> = args.into_iter().map(RespFrame::BulkString).collect();
        let mut final_args = vec![RespFrame::BulkString("CLUSTER".into())];
        final_args.extend(cmd_parts);
        let frame = RespFrame::Array(final_args);

        match self.send_and_receive(frame).await? {
            RespFrame::SimpleString(s) if s.eq_ignore_ascii_case("OK") => Ok(()),
            other => Err(anyhow!("Unexpected response to CLUSTER SETSLOT: {other:?}")),
        }
    }

    /// Sends `CLUSTER GETKEYSINSLOT ...` and parses the resulting array of keys.
    pub async fn get_keys_in_slot(&mut self, slot: u16, count: usize) -> Result<Vec<Bytes>> {
        let frame = Command::Cluster(ClusterInfo {
            subcommand: ClusterSubcommand::GetKeysInSlot { slot, count },
        })
        .into();

        match self.send_and_receive(frame).await? {
            RespFrame::Array(arr) => arr
                .into_iter()
                .map(|frame| match frame {
                    RespFrame::BulkString(bs) => Ok(bs),
                    _ => Err(anyhow!("Expected bulk string in GETKEYSINSLOT reply")),
                })
                .collect(),
            other => Err(anyhow!("Unexpected response to GETKEYSINSLOT: {other:?}")),
        }
    }

    /// Sends a `MIGRATE` command to move a single key.
    pub async fn migrate_key(
        &mut self,
        host: String,
        port: u16,
        key: Bytes,
        db_index: usize,
        timeout_ms: u64,
    ) -> Result<()> {
        let frame = Command::Migrate(crate::core::commands::generic::Migrate {
            host,
            port,
            key,
            db_index,
            timeout_ms,
            copy: false,
            // REPLACE is important for handling cases where a previous migration failed mid-way.
            replace: true,
        })
        .into();

        match self.send_and_receive(frame).await? {
            // MIGRATE returns OK if the key was moved, or NOKEY if it didn't exist.
            // Both are considered success in the context of resharding.
            RespFrame::SimpleString(s)
                if s.eq_ignore_ascii_case("OK") || s.eq_ignore_ascii_case("NOKEY") =>
            {
                Ok(())
            }
            other => Err(anyhow!("Unexpected response to MIGRATE: {other:?}")),
        }
    }
}
