use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::append::Append;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_append_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let append_command = Append::parse(&args).unwrap();
    assert_eq!(append_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(append_command.value, Bytes::from_static(b"myvalue"));
}

#[tokio::test]
async fn test_append_parse_no_args() {
    let args = [];
    let err = Append::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_append_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = Append::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_append_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = Append::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_append_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let err = Append::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_append_parse_non_bulk_string_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::Integer(456),
    ];
    let err = Append::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
