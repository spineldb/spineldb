use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::bitpos::BitPos;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_bitpos_parse_valid_basic() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
    ];
    let bitpos_command = BitPos::parse(&args).unwrap();
    assert_eq!(bitpos_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(bitpos_command.bit, 1);
    assert!(bitpos_command.range.is_none());
}

#[tokio::test]
async fn test_bitpos_parse_valid_with_start() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let bitpos_command = BitPos::parse(&args).unwrap();
    assert_eq!(bitpos_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(bitpos_command.bit, 0);
    assert_eq!(bitpos_command.range, Some((10, -1))); // -1 is default end
}

#[tokio::test]
async fn test_bitpos_parse_valid_with_start_and_end() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"5")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let bitpos_command = BitPos::parse(&args).unwrap();
    assert_eq!(bitpos_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(bitpos_command.bit, 1);
    assert_eq!(bitpos_command.range, Some((5, 10)));
}

#[tokio::test]
async fn test_bitpos_parse_no_args() {
    let args = [];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_bitpos_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_bitpos_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_bitpos_parse_non_integer_bit() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-a-bit")),
    ];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_bitpos_parse_invalid_bit_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"2")),
    ];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_bitpos_parse_non_integer_start() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
    ];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_bitpos_parse_non_integer_end() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
    ];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_bitpos_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let err = BitPos::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
