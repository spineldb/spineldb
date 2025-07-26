use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::rename::Rename;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_rename_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"oldkey")),
        RespFrame::BulkString(Bytes::from_static(b"newkey")),
    ];
    let rename_command = Rename::parse(&args).unwrap();
    assert_eq!(rename_command.source, Bytes::from_static(b"oldkey"));
    assert_eq!(rename_command.destination, Bytes::from_static(b"newkey"));
}

#[tokio::test]
async fn test_rename_parse_no_args() {
    let args = [];
    let err = Rename::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_rename_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"oldkey"))];
    let err = Rename::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_rename_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"oldkey")),
        RespFrame::BulkString(Bytes::from_static(b"newkey")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = Rename::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_rename_parse_non_bulk_string_source() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"newkey")),
    ];
    let err = Rename::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_rename_parse_non_bulk_string_destination() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"oldkey")),
        RespFrame::Integer(456),
    ];
    let err = Rename::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
