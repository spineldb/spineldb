// src/core/protocol/resp_frame.rs

//! Implements the RESP (REdis Serialization Protocol) frame structure and the
//! corresponding `Encoder` and `Decoder` for network communication.

use crate::core::SpinelDBError;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// The CRLF (Carriage Return, Line Feed) sequence used to terminate lines in RESP.
const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = 2;

// Protocol-level limits to prevent denial-of-service attacks from malicious or malformed frames.
const MAX_FRAME_ELEMENTS: usize = 1_024 * 1_024; // Max elements in an array.
const MAX_BULK_STRING_SIZE: usize = 512 * 1024 * 1024; // 512MB max bulk string size.

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

    /// Decodes a `RespFrame` from a `BytesMut` buffer using an iterative, stack-based approach
    /// to prevent stack overflows from deeply nested arrays.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut cursor = 0;
        // The parsing state stack. Each element is `(elements_remaining, frames_collected)`.
        let mut stack: Vec<(usize, Vec<RespFrame>)> = vec![(1, Vec::with_capacity(1))];

        while let Some((remaining, frames)) = stack.last_mut() {
            if *remaining == 0 {
                // This level is complete. Pop, build the array, and add it to the parent.
                let (completed_count, completed_frames) = stack.pop().unwrap();
                let frame = if completed_count > 1 || stack.len() == 1 {
                    RespFrame::Array(completed_frames)
                } else if let Some(frame) = completed_frames.into_iter().next() {
                    // This handles the top-level case where we expect just one frame.
                    frame
                } else {
                    // Should be unreachable if completed_count was > 0.
                    return Err(SpinelDBError::Internal("Empty frame list popped".into()));
                };

                if let Some((_, parent_frames)) = stack.last_mut() {
                    parent_frames.push(frame);
                } else {
                    // This was the last frame on the stack, so it's our final result.
                    src.advance(cursor);
                    return Ok(Some(frame));
                }
                continue;
            }

            // Try to parse one frame at the current level.
            match parse_frame_non_recursive(&src[cursor..]) {
                Ok((Some(frame), frame_len)) => {
                    cursor += frame_len;
                    *remaining -= 1;
                    frames.push(frame);
                }
                Ok((None, frame_len)) => {
                    // This means we encountered an array and need to go deeper.
                    cursor += frame_len;
                    let (arr_len, new_cursor_pos) = parse_array_header(&src[cursor..])?;
                    cursor += new_cursor_pos;

                    if arr_len > 0 {
                        stack.push((arr_len as usize, Vec::with_capacity(arr_len as usize)));
                    } else {
                        // Handle empty or null arrays without pushing to the stack.
                        let array_frame = if arr_len == 0 {
                            RespFrame::Array(vec![])
                        } else {
                            RespFrame::NullArray
                        };
                        *remaining -= 1;
                        frames.push(array_frame);
                    }
                }
                Err(e) => {
                    // If data is incomplete, just return Ok(None) and wait for more.
                    // Otherwise, propagate the error.
                    return if matches!(e, SpinelDBError::IncompleteData) {
                        Ok(None)
                    } else {
                        Err(e)
                    };
                }
            }
        }

        // Should be unreachable if the logic is correct.
        Ok(None)
    }
}

/// Parses the header of an array, returning the number of elements.
fn parse_array_header(src: &[u8]) -> Result<(isize, usize), SpinelDBError> {
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let arr_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if arr_len as usize > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }
    Ok((arr_len, len_of_line + 1))
}

/// A non-recursive frame parser.
/// For arrays, it only parses the header (`*<len>\r\n`) and returns `Ok((None, len))`,
/// signaling to the iterative decoder that it needs to push a new state onto the stack.
fn parse_frame_non_recursive(src: &[u8]) -> Result<(Option<RespFrame>, usize), SpinelDBError> {
    if src.is_empty() {
        return Err(SpinelDBError::IncompleteData);
    }
    match src[0] {
        b'+' => parse_simple_string(src).map(|(f, l)| (Some(f), l)),
        b'-' => parse_error(src).map(|(f, l)| (Some(f), l)),
        b':' => parse_integer(src).map(|(f, l)| (Some(f), l)),
        b'$' => parse_bulk_string(src).map(|(f, l)| (Some(f), l)),
        b'*' => {
            // For arrays, just signal that an array was found. The iterative decoder will handle it.
            Ok((None, 0))
        }
        _ => Err(SpinelDBError::SyntaxError),
    }
}

/// Helper function to find the next CRLF sequence in a buffer.
fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(CRLF_LEN).position(|window| window == CRLF)
}

/// Parses a single line (up to CRLF) from a buffer.
fn parse_line(src: &[u8]) -> Result<(&[u8], usize), SpinelDBError> {
    find_crlf(src)
        .map(|pos| (&src[..pos], pos + CRLF_LEN))
        .ok_or(SpinelDBError::IncompleteData)
}

/// Parses a Simple String (e.g., `+OK\r\n`).
fn parse_simple_string(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len) = parse_line(&src[1..])?;
    Ok((
        RespFrame::SimpleString(String::from_utf8_lossy(line).to_string()),
        len + 1,
    ))
}

/// Parses an Error (e.g., `-ERR message\r\n`).
fn parse_error(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len) = parse_line(&src[1..])?;
    Ok((
        RespFrame::Error(String::from_utf8_lossy(line).to_string()),
        len + 1,
    ))
}

/// Parses an Integer (e.g., `:1000\r\n`).
fn parse_integer(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let i = s.parse::<i64>().map_err(|_| SpinelDBError::SyntaxError)?;
    Ok((RespFrame::Integer(i), len + 1))
}

/// Parses a Bulk String (e.g., `$5\r\nhello\r\n`).
fn parse_bulk_string(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let str_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    // Handle Null Bulk String ($-1\r\n).
    if str_len == -1 {
        return Ok((RespFrame::Null, len_of_line + 1));
    }

    let str_len = str_len as usize;
    if str_len > MAX_BULK_STRING_SIZE {
        return Err(SpinelDBError::SyntaxError);
    }

    let total_len_prefix = len_of_line + 1;
    // Check if the entire bulk string (including its data and final CRLF) is in the buffer.
    if src.len() < total_len_prefix + str_len + CRLF_LEN {
        return Err(SpinelDBError::IncompleteData);
    }

    // Validate the trailing CRLF.
    if &src[total_len_prefix + str_len..total_len_prefix + str_len + CRLF_LEN] != CRLF {
        return Err(SpinelDBError::SyntaxError);
    }

    let data_start = total_len_prefix;
    let data_end = total_len_prefix + str_len;
    let data = Bytes::copy_from_slice(&src[data_start..data_end]);
    Ok((RespFrame::BulkString(data), data_end + CRLF_LEN))
}
