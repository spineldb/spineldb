// src/core/replication/sync.rs

//! Provides helper structs and methods for the initial data synchronization
//! between a primary and a replica.

use crate::core::SpinelDBError;
use bytes::Bytes;
// Import the necessary I/O traits
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// `InitialSyncer` is a helper struct used by a primary to send the initial
/// state (SPLDB snapshot file) to a replica during a full resynchronization.
/// It is generic over the stream type `S`.
pub struct InitialSyncer<'a, S: AsyncWrite + Unpin> {
    stream: &'a mut S,
}

impl<'a, S: AsyncWrite + Unpin> InitialSyncer<'a, S> {
    /// Creates a new `InitialSyncer` with a mutable reference to the replica's stream.
    pub fn new(stream: &'a mut S) -> Self {
        Self { stream }
    }

    /// Sends a serialized SPLDB file to the replica.
    ///
    /// The SPLDB data is sent as a single RESP Bulk String. This involves prefixing the
    /// raw SPLDB bytes with `$<length>\r\n`.
    pub async fn send_snapshot_file(&mut self, spldb_bytes: &Bytes) -> Result<(), SpinelDBError> {
        // First, send the RESP Bulk String header with the length of the SPLDB file.
        let spldb_header = format!("${}\r\n", spldb_bytes.len());
        self.stream.write_all(spldb_header.as_bytes()).await?;

        // Then, send the raw binary content of the SPLDB file.
        self.stream.write_all(spldb_bytes).await?;
        Ok(())
    }
}
