use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::persist::Persist;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_persist_parse_valid_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let persist_command = Persist::parse(&args).unwrap();
    assert_eq!(persist_command.key, Bytes::from_static(b"mykey"));
}

#[tokio::test]
async fn test_persist_parse_no_args() {
    let args = [];
    let err = Persist::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_persist_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Persist::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_persist_parse_non_bulk_string_key() {
    let args = [RespFrame::Integer(123)];
    let err = Persist::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
