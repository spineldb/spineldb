// src/core/warden/client.rs

//! Defines a simple, internal, asynchronous SpinelDB client used by the Warden
//! to communicate with monitored SpinelDB instances.

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

// Import the core RESP types from the main server implementation.
use crate::core::protocol::{RespFrame, RespFrameCodec};
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// An internal client for sending commands to and receiving responses from
/// SpinelDB instances. This is used by a Warden to monitor servers.
#[derive(Debug)]
pub struct WardenClient {
    stream: TcpStream,
    codec: RespFrameCodec,
}

impl WardenClient {
    /// Attempts to connect to a given address with a configured timeout.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await??;
        Ok(Self {
            stream,
            codec: RespFrameCodec,
        })
    }

    /// A generic method to send a RESP frame and wait for a single response frame.
    pub async fn send_and_receive(&mut self, frame: RespFrame) -> Result<RespFrame> {
        // 1. Encode the command frame into a byte buffer.
        let mut write_buf = BytesMut::new();
        self.codec.encode(frame, &mut write_buf)?;

        // 2. Send the encoded command to the server.
        self.stream.write_all(&write_buf).await?;

        // 3. Loop to read the response from the server.
        let mut read_buf = BytesMut::with_capacity(4096);
        loop {
            // Gunakan konstanta READ_TIMEOUT.
            let read_fut = self.stream.read_buf(&mut read_buf);
            match tokio::time::timeout(READ_TIMEOUT, read_fut).await {
                Ok(Ok(0)) => return Err(anyhow!("Connection closed by peer")),
                Ok(Ok(_)) => {
                    if let Some(reply) = self.codec.decode(&mut read_buf)? {
                        return Ok(reply);
                    }
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => return Err(anyhow!("Read timeout while waiting for response")),
            }
        }
    }

    /// Sends a `PING` command and expects a "PONG" simple string response.
    pub async fn ping(&mut self) -> Result<String> {
        let frame = RespFrame::Array(vec![RespFrame::BulkString("PING".into())]);
        let reply = self.send_and_receive(frame).await?;
        match reply {
            RespFrame::SimpleString(s) => Ok(s),
            _ => Err(anyhow!("Unexpected PING reply: {reply:?}")),
        }
    }

    /// Sends an `INFO replication` command and expects a bulk string response.
    pub async fn info_replication(&mut self) -> Result<String> {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString("INFO".into()),
            RespFrame::BulkString("replication".into()),
        ]);
        let reply = self.send_and_receive(frame).await?;
        match reply {
            RespFrame::BulkString(bs) => Ok(String::from_utf8_lossy(&bs).to_string()),
            _ => Err(anyhow!("Unexpected INFO reply: {reply:?}")),
        }
    }
}
