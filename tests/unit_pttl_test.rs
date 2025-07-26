use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::pttl::Pttl;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_pttl_parse_valid() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let pttl_command = Pttl::parse(&args).unwrap();
    assert_eq!(pttl_command.key, Bytes::from_static(b"mykey"));
}

#[tokio::test]
async fn test_pttl_parse_no_args() {
    let args = [];
    let err = Pttl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pttl_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = Pttl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pttl_parse_non_bulk_string_key() {
    let args = [RespFrame::Integer(123)];
    let err = Pttl::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
