use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::type_cmd::TypeInfo;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_type_parse_valid() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let type_command = TypeInfo::parse(&args).unwrap();
    assert_eq!(type_command.key, Bytes::from_static(b"mykey"));
}

#[tokio::test]
async fn test_type_parse_no_args() {
    let args = [];
    let err = TypeInfo::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_type_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = TypeInfo::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_type_parse_non_bulk_string_key() {
    let args = [RespFrame::Integer(123)];
    let err = TypeInfo::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
