use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::setex::SetEx;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_setex_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let setex_command = SetEx::parse(&args).unwrap();
    assert_eq!(setex_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(setex_command.seconds, 60);
    assert_eq!(setex_command.value, Bytes::from_static(b"myvalue"));
}

#[tokio::test]
async fn test_setex_parse_no_args() {
    let args = [];
    let err = SetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setex_parse_too_few_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60")),
    ];
    let err = SetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setex_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = SetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setex_parse_non_integer_seconds() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let err = SetEx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_setex_parse_zero_seconds() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let err = SetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_setex_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"60")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let err = SetEx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_setex_parse_non_bulk_string_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60")),
        RespFrame::Integer(456),
    ];
    let err = SetEx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
