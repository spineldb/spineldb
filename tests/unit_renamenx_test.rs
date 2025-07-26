use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::renamenx::RenameNx;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_renamenx_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"oldkey")),
        RespFrame::BulkString(Bytes::from_static(b"newkey")),
    ];
    let renamenx_command = RenameNx::parse(&args).unwrap();
    assert_eq!(renamenx_command.source, Bytes::from_static(b"oldkey"));
    assert_eq!(renamenx_command.destination, Bytes::from_static(b"newkey"));
}

#[tokio::test]
async fn test_renamenx_parse_no_args() {
    let args = [];
    let err = RenameNx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_renamenx_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"oldkey"))];
    let err = RenameNx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_renamenx_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"oldkey")),
        RespFrame::BulkString(Bytes::from_static(b"newkey")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = RenameNx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_renamenx_parse_non_bulk_string_source() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"newkey")),
    ];
    let err = RenameNx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_renamenx_parse_non_bulk_string_destination() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"oldkey")),
        RespFrame::Integer(456),
    ];
    let err = RenameNx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
