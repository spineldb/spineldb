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
    // RESP3 additions
    Map(Vec<(RespFrame, RespFrame)>),
    Set(Vec<RespFrame>),
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    VerbatimString(String, Bytes),
    Attribute(Vec<(RespFrame, RespFrame)>, Box<RespFrame>),
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
pub struct RespFrameCodec {
    protocol_version: u8,
}

impl RespFrameCodec {
    /// Creates a new `RespFrameCodec` with the specified protocol version.
    pub fn new(protocol_version: u8) -> Self {
        Self { protocol_version }
    }
}

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
            // RESP3 additions - only encode if protocol_version is 3
            RespFrame::Map(map) => {
                if self.protocol_version < 3 {
                    return Err(SpinelDBError::ProtocolError(
                        "Map type not supported in RESP2".to_string(),
                    ));
                }
                dst.extend_from_slice(b"%");
                dst.extend_from_slice(map.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                for (k, v) in map {
                    self.encode(k, dst)?;
                    self.encode(v, dst)?;
                }
            }
            RespFrame::Set(set) => {
                if self.protocol_version < 3 {
                    return Err(SpinelDBError::ProtocolError(
                        "Set type not supported in RESP2".to_string(),
                    ));
                }
                dst.extend_from_slice(b"~");
                dst.extend_from_slice(set.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                for frame in set {
                    self.encode(frame, dst)?;
                }
            }
            RespFrame::Boolean(b) => {
                if self.protocol_version < 3 {
                    return Err(SpinelDBError::ProtocolError(
                        "Boolean type not supported in RESP2".to_string(),
                    ));
                }
                dst.extend_from_slice(if b { b"#t" } else { b"#f" });
                dst.extend_from_slice(CRLF);
            }
            RespFrame::Double(d) => {
                if self.protocol_version < 3 {
                    return Err(SpinelDBError::ProtocolError(
                        "Double type not supported in RESP2".to_string(),
                    ));
                }
                dst.extend_from_slice(b",");
                dst.extend_from_slice(d.to_string().as_bytes());
                dst.extend_from_slice(CRLF);
            }
            RespFrame::BigNumber(bn) => {
                if self.protocol_version < 3 {
                    return Err(SpinelDBError::ProtocolError(
                        "BigNumber type not supported in RESP2".to_string(),
                    ));
                }
                dst.extend_from_slice(b"(");
                dst.extend_from_slice(bn.as_bytes());
                dst.extend_from_slice(CRLF);
            }
            RespFrame::VerbatimString(format, text) => {
                if self.protocol_version < 3 {
                    return Err(SpinelDBError::ProtocolError(
                        "VerbatimString type not supported in RESP2".to_string(),
                    ));
                }
                dst.extend_from_slice(b"=");
                dst.extend_from_slice(text.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                dst.extend_from_slice(format.as_bytes());
                dst.extend_from_slice(b":");
                dst.extend_from_slice(&text);
                dst.extend_from_slice(CRLF);
            }
            RespFrame::Attribute(attrs, data) => {
                if self.protocol_version < 3 {
                    return Err(SpinelDBError::ProtocolError(
                        "Attribute type not supported in RESP2".to_string(),
                    ));
                }
                dst.extend_from_slice(b"|");
                dst.extend_from_slice(attrs.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                for (k, v) in attrs {
                    self.encode(k, dst)?;
                    self.encode(v, dst)?;
                }
                self.encode(*data, dst)?;
            }
        }
        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;
    type Error = SpinelDBError;

    /// Decodes a `RespFrame` from a `BytesMut` buffer.
    ///
    /// It returns `Ok(None)` if the buffer does not contain a full frame yet,
    /// allowing the `Framed` stream to wait for more data from the network.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match parse_frame(src, self.protocol_version) {
            Ok((frame, len)) => {
                // Advance the buffer past the successfully parsed frame.
                src.advance(len);
                Ok(Some(frame))
            }
            // If the data is incomplete, wait for more data.
            Err(SpinelDBError::IncompleteData) => Ok(None),
            // For other errors, propagate them up to the connection handler.
            Err(e) => Err(e),
        }
    }
}

/// The main parsing entry point. It inspects the first byte (the type prefix)
/// and dispatches to the appropriate parsing function.
fn parse_frame(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if src.is_empty() {
        return Err(SpinelDBError::IncompleteData);
    }
    match src[0] {
        b'+' => parse_simple_string(src),
        b'-' => parse_error(src),
        b':' => parse_integer(src),
        b'
        b'*' => parse_array(src),
        // RESP3 additions
        b'%' => parse_map(src),
        b'~' => parse_set(src),
        b'#' => parse_boolean(src),
        b',' => parse_double(src),
        b'(' => parse_big_number(src),
        b'=' => parse_verbatim_string(src),
        b'|' => parse_attribute(src),
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

/// Parses an Array (e.g., `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`).
fn parse_array(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let arr_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    // Handle Null Array (*-1\r\n).
    if arr_len == -1 {
        return Ok((RespFrame::NullArray, len_of_line + 1));
    }

    let arr_len = arr_len as usize;
    if arr_len > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut frames = Vec::with_capacity(arr_len);
    let mut cursor = len_of_line + 1;

    // Recursively parse each element of the array.
    for _ in 0..arr_len {
        let (frame, frame_len) = parse_frame(&src[cursor..], protocol_version)?;
        frames.push(frame);
        cursor += frame_len;
    }

    Ok((RespFrame::Array(frames), cursor))
}

// --- RESP3 Parsing Functions ---

/// Parses a Map (e.g., `%2\r\n+key1\r\n+value1\r\n+key2\r\n+value2\r\n`).
fn parse_map(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if protocol_version < 3 {
        return Err(SpinelDBError::ProtocolError(
            "Map type not supported in RESP2".to_string(),
        ));
    }
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let map_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if map_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let map_len = map_len as usize;
    if map_len > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut map_entries = Vec::with_capacity(map_len);
    let mut cursor = len_of_line + 1;

    for _ in 0..map_len {
        let (key_frame, key_len) = parse_frame(&src[cursor..], protocol_version)?;
        cursor += key_len;
        let (value_frame, value_len) = parse_frame(&src[cursor..], protocol_version)?;
        cursor += value_len;
        map_entries.push((key_frame, value_frame));
    }

    Ok((RespFrame::Map(map_entries), cursor))
}

/// Parses a Set (e.g., `~2\r\n+element1\r\n+element2\r\n`).
fn parse_set(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if protocol_version < 3 {
        return Err(SpinelDBError::ProtocolError(
            "Set type not supported in RESP2".to_string(),
        ));
    }
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let set_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if set_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let set_len = set_len as usize;
    if set_len > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut set_elements = Vec::with_capacity(set_len);
    let mut cursor = len_of_line + 1;

    for _ in 0..set_len {
        let (element_frame, element_len) = parse_frame(&src[cursor..], protocol_version)?;
        set_elements.push(element_frame);
        cursor += element_len;
    }

    Ok((RespFrame::Set(set_elements), cursor))
}

/// Parses a Boolean (e.g., `#t\r\n` or `#f\r\n`).
fn parse_boolean(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if protocol_version < 3 {
        return Err(SpinelDBError::ProtocolError(
            "Boolean type not supported in RESP2".to_string(),
        ));
    }
    let (line, len) = parse_line(&src[1..])?;
    match line {
        b"t" => Ok((RespFrame::Boolean(true), len + 1)),
        b"f" => Ok((RespFrame::Boolean(false), len + 1)),
        _ => Err(SpinelDBError::SyntaxError),
    }
}

/// Parses a Double (e.g., `,1.23\r\n`).
fn parse_double(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if protocol_version < 3 {
        return Err(SpinelDBError::ProtocolError(
            "Double type not supported in RESP2".to_string(),
        ));
    }
    let (line, len) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let d = s.parse::<f64>().map_err(|_| SpinelDBError::SyntaxError)?;
    Ok((RespFrame::Double(d), len + 1))
}

/// Parses a Big Number (e.g., `(12345678901234567890\r\n`).
fn parse_big_number(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if protocol_version < 3 {
        return Err(SpinelDBError::ProtocolError(
            "BigNumber type not supported in RESP2".to_string(),
        ));
    }
    let (line, len) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line).to_string();
    // Basic validation: ensure it's a valid number string.
    if s.is_empty() || !s.chars().all(|c| c.is_ascii_digit() || c == '-' || c == '+') {
        return Err(SpinelDBError::SyntaxError);
    }
    Ok((RespFrame::BigNumber(s), len + 1))
}

/// Parses a Verbatim String (e.g., `=15\r\ntxt:Some text\r\n`).
fn parse_verbatim_string(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if protocol_version < 3 {
        return Err(SpinelDBError::ProtocolError(
            "VerbatimString type not supported in RESP2".to_string(),
        ));
    }
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let str_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if str_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let str_len = str_len as usize;
    if str_len > MAX_BULK_STRING_SIZE {
        return Err(SpinelDBError::SyntaxError);
    }

    let total_len_prefix = len_of_line + 1;
    if src.len() < total_len_prefix + str_len + CRLF_LEN {
        return Err(SpinelDBError::IncompleteData);
    }

    if &src[total_len_prefix + str_len..total_len_prefix + str_len + CRLF_LEN] != CRLF {
        return Err(SpinelDBError::SyntaxError);
    }

    let data_start = total_len_prefix;
    let data_end = total_len_prefix + str_len;
    let data_slice = &src[data_start..data_end];

    // Find the first colon to split format and text.
    let colon_pos = data_slice
        .iter()
        .position(|&b| b == b':')
        .ok_or(SpinelDBError::SyntaxError)?;

    let format = String::from_utf8_lossy(&data_slice[..colon_pos]).to_string();
    let text = Bytes::copy_from_slice(&data_slice[colon_pos + 1..]);

    Ok((RespFrame::VerbatimString(format, text), data_end + CRLF_LEN))
}

/// Parses an Attribute (e.g., `|1\r\n+key\r\n+value\r\n$5\r\nhello\r\n`).
fn parse_attribute(src: &[u8], protocol_version: u8) -> Result<(RespFrame, usize), SpinelDBError> {
    if protocol_version < 3 {
        return Err(SpinelDBError::ProtocolError(
            "Attribute type not supported in RESP2".to_string(),
        ));
    }
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let attr_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if attr_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let attr_len = attr_len as usize;
    if attr_len > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut attributes = Vec::with_capacity(attr_len);
    let mut cursor = len_of_line + 1;

    for _ in 0..attr_len {
        let (key_frame, key_len) = parse_frame(&src[cursor..], protocol_version)?;
        cursor += key_len;
        let (value_frame, value_len) = parse_frame(&src[cursor..], protocol_version)?;
        cursor += value_len;
        attributes.push((key_frame, value_frame));
    }

    // The last part of an attribute is the actual data frame.
    let (data_frame, data_len) = parse_frame(&src[cursor..], protocol_version)?;
    cursor += data_len;

    Ok((RespFrame::Attribute(attributes, Box::new(data_frame)), cursor))
}

// --- RESP3 Parsing Functions ---

/// Parses a Map (e.g., `%2\r\n+key1\r\n+value1\r\n+key2\r\n+value2\r\n`).
fn parse_map(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let map_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if map_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let map_len = map_len as usize;
    if map_len > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut map_entries = Vec::with_capacity(map_len);
    let mut cursor = len_of_line + 1;

    for _ in 0..map_len {
        let (key_frame, key_len) = parse_frame(&src[cursor..])?;
        cursor += key_len;
        let (value_frame, value_len) = parse_frame(&src[cursor..])?;
        cursor += value_len;
        map_entries.push((key_frame, value_frame));
    }

    Ok((RespFrame::Map(map_entries), cursor))
}

/// Parses a Set (e.g., `~2\r\n+element1\r\n+element2\r\n`).
fn parse_set(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let set_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if set_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let set_len = set_len as usize;
    if set_len > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut set_elements = Vec::with_capacity(set_len);
    let mut cursor = len_of_line + 1;

    for _ in 0..set_len {
        let (element_frame, element_len) = parse_frame(&src[cursor..])?;
        set_elements.push(element_frame);
        cursor += element_len;
    }

    Ok((RespFrame::Set(set_elements), cursor))
}

/// Parses a Boolean (e.g., `#t\r\n` or `#f\r\n`).
fn parse_boolean(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len) = parse_line(&src[1..])?;
    match line {
        b"t" => Ok((RespFrame::Boolean(true), len + 1)),
        b"f" => Ok((RespFrame::Boolean(false), len + 1)),
        _ => Err(SpinelDBError::SyntaxError),
    }
}

/// Parses a Double (e.g., `,1.23\r\n`).
fn parse_double(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let d = s.parse::<f64>().map_err(|_| SpinelDBError::SyntaxError)?;
    Ok((RespFrame::Double(d), len + 1))
}

/// Parses a Big Number (e.g., `(12345678901234567890\r\n`).
fn parse_big_number(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line).to_string();
    // Basic validation: ensure it's a valid number string.
    if s.is_empty() || !s.chars().all(|c| c.is_ascii_digit() || c == '-' || c == '+') {
        return Err(SpinelDBError::SyntaxError);
    }
    Ok((RespFrame::BigNumber(s), len + 1))
}

/// Parses a Verbatim String (e.g., `=15\r\ntxt:Some text\r\n`).
fn parse_verbatim_string(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let str_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if str_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let str_len = str_len as usize;
    if str_len > MAX_BULK_STRING_SIZE {
        return Err(SpinelDBError::SyntaxError);
    }

    let total_len_prefix = len_of_line + 1;
    if src.len() < total_len_prefix + str_len + CRLF_LEN {
        return Err(SpinelDBError::IncompleteData);
    }

    if &src[total_len_prefix + str_len..total_len_prefix + str_len + CRLF_LEN] != CRLF {
        return Err(SpinelDBError::SyntaxError);
    }

    let data_start = total_len_prefix;
    let data_end = total_len_prefix + str_len;
    let data_slice = &src[data_start..data_end];

    // Find the first colon to split format and text.
    let colon_pos = data_slice
        .iter()
        .position(|&b| b == b':')
        .ok_or(SpinelDBError::SyntaxError)?;

    let format = String::from_utf8_lossy(&data_slice[..colon_pos]).to_string();
    let text = Bytes::copy_from_slice(&data_slice[colon_pos + 1..]);

    Ok((RespFrame::VerbatimString(format, text), data_end + CRLF_LEN))
}

/// Parses an Attribute (e.g., `|1\r\n+key\r\n+value\r\n$5\r\nhello\r\n`).
fn parse_attribute(src: &[u8]) -> Result<(RespFrame, usize), SpinelDBError> {
    let (line, len_of_line) = parse_line(&src[1..])?;
    let s = String::from_utf8_lossy(line);
    let attr_len = s.parse::<isize>().map_err(|_| SpinelDBError::SyntaxError)?;

    if attr_len < 0 {
        return Err(SpinelDBError::SyntaxError);
    }

    let attr_len = attr_len as usize;
    if attr_len > MAX_FRAME_ELEMENTS {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut attributes = Vec::with_capacity(attr_len);
    let mut cursor = len_of_line + 1;

    for _ in 0..attr_len {
        let (key_frame, key_len) = parse_frame(&src[cursor..])?;
        cursor += key_len;
        let (value_frame, value_len) = parse_frame(&src[cursor..])?;
        cursor += value_len;
        attributes.push((key_frame, value_frame));
    }

    // The last part of an attribute is the actual data frame.
    let (data_frame, data_len) = parse_frame(&src[cursor..])?;
    cursor += data_len;

    Ok((RespFrame::Attribute(attributes, Box::new(data_frame)), cursor))
}
