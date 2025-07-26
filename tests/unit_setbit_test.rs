use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::setbit::SetBit;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_setbit_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
    ];
    let setbit_command = SetBit::parse(&args).unwrap();
    assert_eq!(setbit_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(setbit_command.offset, 10);
    assert_eq!(setbit_command.value, 1);
}

#[tokio::test]
async fn test_setbit_parse_valid_zero_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"20")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let setbit_command = SetBit::parse(&args).unwrap();
    assert_eq!(setbit_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(setbit_command.offset, 20);
    assert_eq!(setbit_command.value, 0);
}

#[tokio::test]
async fn test_setbit_parse_no_args() {
    let args = [];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setbit_parse_too_few_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setbit_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_setbit_parse_non_integer_offset() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
    ];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_setbit_parse_negative_offset() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"-10")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
    ];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_setbit_parse_non_integer_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
    ];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_setbit_parse_invalid_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"2")),
    ];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_setbit_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
    ];
    let err = SetBit::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
