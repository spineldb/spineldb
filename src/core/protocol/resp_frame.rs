// src/core/protocol/resp_frame.rs

//! Implements the RESP (REdis Serialization Protocol) frame structure and the
//! corresponding `Encoder` and `Decoder` for network communication.

use crate::core::SpinelDBError;
use bytes::{Buf, Bytes, BytesMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::codec::{Decoder, Encoder};

/// The CRLF (Carriage Return, Line Feed) sequence used to terminate lines in RESP.
const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = 2;

// Protocol-level limits to prevent denial-of-service attacks.
const MAX_FRAME_ELEMENTS: usize = 1_024 * 1_024; // Max elements in an array.
const DEFAULT_MAX_BULK_STRING_SIZE: usize = 512 * 1024 * 1024; // 512MB default.
const MAX_RECURSION_DEPTH: usize = 256; // Limit recursion to prevent stack overflow.
/// Hard cap. Even when configured, the user can never raise the bulk string
/// limit above this value.
const ABSOLUTE_MAX_BULK_STRING_SIZE: usize = 16 * 1024 * 1024 * 1024; // 16 GiB.

/// The currently active maximum bulk string size, in bytes.
///
/// This is an atomic so that `CONFIG SET safety.max_bulk_string_size <n>`
/// can take effect without rebuilding the codec. The default value is
/// [`DEFAULT_MAX_BULK_STRING_SIZE`].
pub static MAX_BULK_STRING_SIZE: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_BULK_STRING_SIZE);

/// Configures the active maximum bulk string size. Values above
/// [`ABSOLUTE_MAX_BULK_STRING_SIZE`] are clamped.
pub fn set_max_bulk_string_size(bytes: usize) {
    let clamped = bytes.clamp(1, ABSOLUTE_MAX_BULK_STRING_SIZE);
    MAX_BULK_STRING_SIZE.store(clamped, Ordering::Relaxed);
}

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
        if str_len > MAX_BULK_STRING_SIZE.load(Ordering::Relaxed) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::SpinelDBError;
    use bytes::BytesMut;
    use std::sync::Mutex;

    /// Serializes tests that mutate the global `MAX_BULK_STRING_SIZE` so they
    /// do not race with any test that depends on its default value.
    static MAX_BULK_STRING_SIZE_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn decode_one(input: &[u8]) -> Result<Option<RespFrame>, SpinelDBError> {
        let mut buf = BytesMut::from(input);
        RespFrameCodec.decode(&mut buf)
    }

    fn encode(frame: RespFrame) -> Vec<u8> {
        let mut buf = BytesMut::new();
        RespFrameCodec.encode(frame, &mut buf).unwrap();
        buf.to_vec()
    }

    #[test]
    fn test_encode_decode_simple_string() {
        let frame = RespFrame::SimpleString("OK".to_string());
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"+OK\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_error() {
        let frame = RespFrame::Error("ERR something went wrong".to_string());
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"-ERR something went wrong\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_integer() {
        for n in [0i64, 1, -1, 42, -42, i64::MAX, i64::MIN] {
            let frame = RespFrame::Integer(n);
            let encoded = encode(frame.clone());
            assert_eq!(encoded, format!(":{n}\r\n").as_bytes());
            let decoded = decode_one(&encoded).unwrap().unwrap();
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn test_encode_decode_bulk_string() {
        let frame = RespFrame::BulkString(Bytes::from_static(b"hello"));
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"$5\r\nhello\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_empty_bulk_string() {
        let frame = RespFrame::BulkString(Bytes::new());
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"$0\r\n\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_null_bulk_string() {
        let frame = RespFrame::Null;
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"$-1\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_null_array() {
        let frame = RespFrame::NullArray;
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"*-1\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_empty_array() {
        let frame = RespFrame::Array(vec![]);
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"*0\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_nested_array() {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(b"GET")),
            RespFrame::BulkString(Bytes::from_static(b"mykey")),
        ]);
        let encoded = encode(frame.clone());
        assert_eq!(encoded, b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_deeply_nested_array() {
        let frame = RespFrame::Array(vec![
            RespFrame::Integer(1),
            RespFrame::Array(vec![
                RespFrame::Integer(2),
                RespFrame::Array(vec![RespFrame::Integer(3)]),
            ]),
        ]);
        let encoded = encode(frame.clone());
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_encode_decode_array_with_mixed_types() {
        let frame = RespFrame::Array(vec![
            RespFrame::SimpleString("PONG".to_string()),
            RespFrame::Integer(42),
            RespFrame::BulkString(Bytes::from_static(b"data")),
            RespFrame::Null,
            RespFrame::NullArray,
        ]);
        let encoded = encode(frame.clone());
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_decode_empty_buffer_returns_none() {
        let result = decode_one(b"").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_incomplete_returns_none() {
        // Just the prefix
        let result = decode_one(b"$5").unwrap();
        assert!(result.is_none());
        // Prefix and length but no data
        let result = decode_one(b"$5\r\n").unwrap();
        assert!(result.is_none());
        // Prefix, length, partial data
        let result = decode_one(b"$5\r\nhel").unwrap();
        assert!(result.is_none());
        // Prefix, length, data, but no trailing CRLF
        let result = decode_one(b"$5\r\nhello").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_invalid_type_byte_returns_syntax_error() {
        let result = decode_one(b"?garbage\r\n");
        assert!(matches!(result, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_decode_non_integer_bulk_string_length() {
        let result = decode_one(b"$abc\r\n");
        assert!(matches!(result, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_decode_missing_crlf_after_bulk_data() {
        let result = decode_one(b"$3\r\nfooXX");
        assert!(matches!(result, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_decode_non_integer_array_length() {
        let result = decode_one(b"*xyz\r\n");
        assert!(matches!(result, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_decode_non_integer_value() {
        let result = decode_one(b":notanumber\r\n");
        assert!(matches!(result, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_decode_array_too_large() {
        let huge = format!("*{}\r\n", MAX_FRAME_ELEMENTS + 1);
        let result = decode_one(huge.as_bytes());
        assert!(matches!(result, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_decode_bulk_string_exceeds_configured_limit() {
        // Temporarily lower the cap and ensure that oversized bulk strings are rejected.
        // We pick a limit (8) that is above the size of all bulk strings used
        // by other tests in this module (the largest is 6 bytes), so we don't
        // disrupt parallel tests.
        let _guard = MAX_BULK_STRING_SIZE_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let original = MAX_BULK_STRING_SIZE.load(Ordering::Relaxed);
        set_max_bulk_string_size(8);
        let r = decode_one(b"$100\r\n");
        set_max_bulk_string_size(original);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_decode_recursion_depth_limit() {
        // Build a deeply nested array just past the recursion limit.
        let mut input = String::new();
        let depth = MAX_RECURSION_DEPTH + 5;
        for _ in 0..depth {
            input.push_str("*1\r\n");
        }
        input.push_str("$1\r\nX\r\n");
        let result = decode_one(input.as_bytes());
        assert!(matches!(result, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_decode_multiple_frames_in_sequence() {
        // The decoder should return the first complete frame and leave the rest in the buffer.
        let mut buf = BytesMut::from(&b"+OK\r\n:42\r\n"[..]);
        let first = RespFrameCodec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(first, RespFrame::SimpleString("OK".to_string()));
        let second = RespFrameCodec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(second, RespFrame::Integer(42));
        assert!(RespFrameCodec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_encode_to_vec_helper() {
        let frame = RespFrame::Array(vec![RespFrame::BulkString(Bytes::from_static(b"PING"))]);
        let bytes = frame.encode_to_vec().unwrap();
        assert_eq!(bytes, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn test_set_max_bulk_string_size_clamps_to_absolute_max() {
        // Setting an absurdly large value should clamp to ABSOLUTE_MAX_BULK_STRING_SIZE.
        // Restore the previous value so we don't break parallel tests.
        let _guard = MAX_BULK_STRING_SIZE_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let original = MAX_BULK_STRING_SIZE.load(Ordering::Relaxed);
        set_max_bulk_string_size(usize::MAX);
        assert_eq!(
            MAX_BULK_STRING_SIZE.load(Ordering::Relaxed),
            ABSOLUTE_MAX_BULK_STRING_SIZE
        );
        set_max_bulk_string_size(original);
    }

    #[test]
    fn test_set_max_bulk_string_size_clamps_to_minimum() {
        // Setting a value of 0 should be clamped to at least 1.
        // Restore the previous value so we don't break parallel tests.
        let _guard = MAX_BULK_STRING_SIZE_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let original = MAX_BULK_STRING_SIZE.load(Ordering::Relaxed);
        set_max_bulk_string_size(0);
        assert!(MAX_BULK_STRING_SIZE.load(Ordering::Relaxed) >= 1);
        set_max_bulk_string_size(original);
    }

    #[test]
    fn test_bulk_string_with_binary_data() {
        let payload = Bytes::from_static(&[0u8, 1, 2, 3, 0xff, 0xfe]);
        let frame = RespFrame::BulkString(payload.clone());
        let encoded = encode(frame.clone());
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(decoded, RespFrame::BulkString(payload));
    }

    #[test]
    fn test_unicode_bulk_string() {
        // Use a small multibyte string to verify that non-ASCII payloads
        // round-trip cleanly through the bulk-string codec.
        let payload: Vec<u8> = vec![b'h', 0xC3, 0xA9, b'l', b'l', b'o'];
        assert_eq!(payload.len(), 6);
        let frame = RespFrame::BulkString(Bytes::from(payload.clone()));
        let encoded = encode(frame.clone());
        // $ + len + CRLF + data + CRLF = 12 bytes total.
        assert_eq!(encoded.len(), 12);
        // Verify the framing structure.
        assert_eq!(&encoded[0..1], b"$");
        assert_eq!(&encoded[1..2], b"6");
        assert_eq!(&encoded[2..4], b"\r\n");
        assert_eq!(&encoded[4..10], &payload[..]);
        assert_eq!(&encoded[10..12], b"\r\n");
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn test_bulk_string_with_crlf_in_payload() {
        // A bulk string's content may contain CR/LF bytes. The decoder must
        // not mistake them for the frame terminator: it uses the length prefix
        // to know exactly when the payload ends.
        let payload = Bytes::copy_from_slice(&[0x0D, 0x0A, b'a', b'b', 0x0D, 0x0A]);
        let frame = RespFrame::BulkString(payload.clone());
        let encoded = encode(frame.clone());
        // 1 ($) + 1 (1) + 2 (CRLF) + 6 (data) + 2 (CRLF) = 12 bytes.
        assert_eq!(encoded.len(), 12);
        let decoded = decode_one(&encoded).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }
}
