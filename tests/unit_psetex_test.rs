use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::psetex::PSetEx;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_psetex_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60000")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let psetex_command = PSetEx::parse(&args).unwrap();
    assert_eq!(psetex_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(psetex_command.milliseconds, 60000);
    assert_eq!(psetex_command.value, Bytes::from_static(b"myvalue"));
}

#[tokio::test]
async fn test_psetex_parse_no_args() {
    let args = [];
    let err = PSetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_psetex_parse_too_few_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60000")),
    ];
    let err = PSetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_psetex_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60000")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = PSetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_psetex_parse_non_integer_milliseconds() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let err = PSetEx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_psetex_parse_zero_milliseconds() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let err = PSetEx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_psetex_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"60000")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let err = PSetEx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_psetex_parse_non_bulk_string_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"60000")),
        RespFrame::Integer(456),
    ];
    let err = PSetEx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
