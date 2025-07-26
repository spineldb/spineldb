use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::setrange::SetRange;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_setrange_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"5")),
        RespFrame::BulkString(Bytes::from_static(b"newvalue")),
    ];
    let setrange_command = SetRange::parse(&args).unwrap();
    assert_eq!(setrange_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(setrange_command.offset, 5);
    assert_eq!(setrange_command.value, Bytes::from_static(b"newvalue"));
}

#[tokio::test]
async fn test_setrange_parse_no_args() {
    let args = [];
    let err = SetRange::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setrange_parse_too_few_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let err = SetRange::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setrange_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"value")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = SetRange::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setrange_parse_non_integer_offset() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
        RespFrame::BulkString(Bytes::from_static(b"value")),
    ];
    let err = SetRange::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_setrange_parse_negative_offset() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"-5")),
        RespFrame::BulkString(Bytes::from_static(b"value")),
    ];
    let err = SetRange::parse(&args).unwrap_err();
    // Assuming negative offset is treated as NotAnInteger or similar error
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_setrange_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"value")),
    ];
    let err = SetRange::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_setrange_parse_non_bulk_string_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::Integer(456),
    ];
    let err = SetRange::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
