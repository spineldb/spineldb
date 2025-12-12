// src/core/protocol/resp_frame.rs

//! Implements the RESP (REdis Serialization Protocol) frame structure and the
//! corresponding `Encoder` and `Decoder` for network communication.

use crate::core::SpinelDBError;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// The CRLF (Carriage Return, Line Feed) sequence used to terminate lines in RESP.
const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = 2;

// Protocol-level limits to prevent denial-of-service attacks.
const MAX_FRAME_ELEMENTS: usize = 1_024 * 1_024; // Max elements in an array.
const MAX_BULK_STRING_SIZE: usize = 512 * 1024 * 1024; // 512MB max bulk string size.
const MAX_RECURSION_DEPTH: usize = 256; // Limit recursion to prevent stack overflow.

/// An enum representing a single frame in the RESP protocol.
/// This is the low-level representation of data exchanged between the client and server.
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Null,
    NullArray,
    Array(Vec<RespFrame>),
}

impl RespFrame {
    /// A convenience method to encode a frame into a `Vec<u8>`.
    /// Useful for replication and AOF persistence where a complete byte vector is needed.
    pub fn encode_to_vec(&self) -> Result<Vec<u8>, SpinelDBError> {
        let mut buf = BytesMut::new();
        RespFrameCodec.encode(self.clone(), &mut buf)?;
        Ok(buf.to_vec())
    }
}

/// A `tokio_util::codec` implementation for encoding and decoding `RespFrame`s.
#[derive(Debug)]
pub struct RespFrameCodec;

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = SpinelDBError;

    /// Encodes a `RespFrame` into a `BytesMut` buffer according to the RESP specification.
    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespFrame::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(CRLF);
            }
            RespFrame::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(CRLF);
            }
            RespFrame::Integer(i) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(i.to_string().as_bytes());
                dst.extend_from_slice(CRLF);
            }
            RespFrame::BulkString(b) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(b.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                dst.extend_from_slice(&b);
                dst.extend_from_slice(CRLF);
            }
            RespFrame::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            RespFrame::NullArray => {
                dst.extend_from_slice(b"*-1\r\n");
            }
            RespFrame::Array(arr) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(arr.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                for frame in arr {
                    // Recursively encode each frame in the array.
                    self.encode(frame, dst)?;
                }
            }
        }
        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;
    type Error = SpinelDBError;

    /// Decodes a `RespFrame` from a `BytesMut` buffer. This function is the entry point
    /// that delegates to a recursive helper to parse the frame.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut bytes = &src[..];
        match self.decode_recursive(&mut bytes, 0) {
            Ok(frame) => {
                let len = src.len() - bytes.len();
                src.advance(len);
                Ok(Some(frame))
            }
            // If the error is `IncompleteData`, we return `Ok(None)` to signal that
            // we need more data. For any other error, we propagate it up.
            Err(SpinelDBError::IncompleteData) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl RespFrameCodec {
    /// A recursive helper function to decode a `RespFrame`.
    /// The `bytes` parameter is a mutable slice that is advanced as it's parsed.
    /// `depth` tracks recursion level to prevent stack overflow.
    fn decode_recursive(
        &self,
        bytes: &mut &[u8],
        depth: usize,
    ) -> Result<RespFrame, SpinelDBError> {
        if depth > MAX_RECURSION_DEPTH {
            return Err(SpinelDBError::InvalidRequest(
                "RESP recursion depth limit exceeded".to_string(),
            ));
        }

        if bytes.is_empty() {
            return Err(SpinelDBError::IncompleteData);
        }

        match bytes[0] {
            b'+' => self.parse_simple_string(bytes),
            b'-' => self.parse_error(bytes),
            b':' => self.parse_integer(bytes),
            b'$' => self.parse_bulk_string(bytes),
            b'*' => self.parse_array(bytes, depth),
            _ => Err(SpinelDBError::SyntaxError),
        }
    }

    /// Finds the next CRLF and returns the line and its total length (including CRLF).
    fn parse_line<'a>(&self, bytes: &mut &'a [u8]) -> Result<&'a [u8], SpinelDBError> {
        if let Some(pos) = find_crlf(bytes) {
            let line = &bytes[..pos];
            // Advance the buffer past the line and CRLF.
            *bytes = &bytes[pos + CRLF_LEN..];
            Ok(line)
        } else {
            Err(SpinelDBError::IncompleteData)
        }
    }

    /// Parses a Simple String (e.g., `+OK\r\n`).
    fn parse_simple_string(&self, bytes: &mut &[u8]) -> Result<RespFrame, SpinelDBError> {
        // Advance past the '+' prefix.
        *bytes = &bytes[1..];
        let line = self.parse_line(bytes)?;
        Ok(RespFrame::SimpleString(
            String::from_utf8_lossy(line).to_string(),
        ))
    }

    /// Parses an Error (e.g., `-ERR message\r\n`).
    fn parse_error(&self, bytes: &mut &[u8]) -> Result<RespFrame, SpinelDBError> {
        // Advance past the '-' prefix.
        *bytes = &bytes[1..];
        let line = self.parse_line(bytes)?;
        Ok(RespFrame::Error(String::from_utf8_lossy(line).to_string()))
    }

    /// Parses an Integer (e.g., `:1000\r\n`).
    fn parse_integer(&self, bytes: &mut &[u8]) -> Result<RespFrame, SpinelDBError> {
        // Advance past the ':' prefix.
        *bytes = &bytes[1..];
        let line = self.parse_line(bytes)?;
        let s = String::from_utf8_lossy(line);
        let i = s.parse::<i64>().map_err(|_| SpinelDBError::SyntaxError)?;
        Ok(RespFrame::Integer(i))
    }

    /// Parses a Bulk String (e.g., `$5\r\nhello\r\n`).
    fn parse_bulk_string(&self, bytes: &mut &[u8]) -> Result<RespFrame, SpinelDBError> {
        // Advance past the '$' prefix.
        *bytes = &bytes[1..];
        let line = self.parse_line(bytes)?;
        let s = String::from_utf8_lossy(line);
        let str_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

        if str_len == -1 {
            return Ok(RespFrame::Null);
        }

        let str_len = str_len as usize;
        if str_len > MAX_BULK_STRING_SIZE {
            return Err(SpinelDBError::SyntaxError);
        }

        if bytes.len() < str_len + CRLF_LEN {
            return Err(SpinelDBError::IncompleteData);
        }

        if &bytes[str_len..str_len + CRLF_LEN] != CRLF {
            return Err(SpinelDBError::SyntaxError);
        }

        let data = Bytes::copy_from_slice(&bytes[..str_len]);
        // Advance the buffer past the data and the final CRLF.
        *bytes = &bytes[str_len + CRLF_LEN..];
        Ok(RespFrame::BulkString(data))
    }

    /// Parses an Array (e.g., `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`).
    fn parse_array(&self, bytes: &mut &[u8], depth: usize) -> Result<RespFrame, SpinelDBError> {
        // Advance past the '*' prefix.
        *bytes = &bytes[1..];
        let line = self.parse_line(bytes)?;
        let s = String::from_utf8_lossy(line);
        let arr_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

        if arr_len == -1 {
            return Ok(RespFrame::NullArray);
        }

        let arr_len = arr_len as usize;
        if arr_len > MAX_FRAME_ELEMENTS {
            return Err(SpinelDBError::SyntaxError);
        }

        let mut frames = Vec::with_capacity(arr_len);
        for _ in 0..arr_len {
            frames.push(self.decode_recursive(bytes, depth + 1)?);
        }
        Ok(RespFrame::Array(frames))
    }
}

/// Helper function to find the next CRLF sequence in a buffer.
fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(CRLF_LEN).position(|window| window == CRLF)
}
