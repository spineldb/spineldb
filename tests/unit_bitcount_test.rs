use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::bitcount::BitCount;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_bitcount_parse_only_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let bitcount_command = BitCount::parse(&args).unwrap();
    assert_eq!(bitcount_command.key, Bytes::from_static(b"mykey"));
    assert!(bitcount_command.range.is_none());
}

#[tokio::test]
async fn test_bitcount_parse_with_range() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
    ];
    let bitcount_command = BitCount::parse(&args).unwrap();
    assert_eq!(bitcount_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(bitcount_command.range, Some((0, -1)));
}

#[tokio::test]
async fn test_bitcount_parse_no_args() {
    let args = [];
    let err = BitCount::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_bitcount_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = BitCount::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_bitcount_parse_only_start_arg() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let err = BitCount::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("SyntaxError"));
}

#[tokio::test]
async fn test_bitcount_parse_non_integer_start() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
    ];
    let err = BitCount::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_bitcount_parse_non_integer_end() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
    ];
    let err = BitCount::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_bitcount_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
    ];
    let err = BitCount::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
