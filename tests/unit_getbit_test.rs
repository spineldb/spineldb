use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::getbit::GetBit;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_getbit_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let getbit_command = GetBit::parse(&args).unwrap();
    assert_eq!(getbit_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(getbit_command.offset, 10);
}

#[tokio::test]
async fn test_getbit_parse_no_args() {
    let args = [];
    let err = GetBit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_getbit_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = GetBit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_getbit_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = GetBit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_getbit_parse_non_integer_offset() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
    ];
    let err = GetBit::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_getbit_parse_negative_offset() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"-10")),
    ];
    let err = GetBit::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_getbit_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let err = GetBit::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
