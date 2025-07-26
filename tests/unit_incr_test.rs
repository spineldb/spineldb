use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::incr::Incr;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_incr_parse_valid() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let incr_command = Incr::parse(&args).unwrap();
    assert_eq!(incr_command.key, Bytes::from_static(b"mykey"));
}

#[tokio::test]
async fn test_incr_parse_no_args() {
    let args = [];
    let err = Incr::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incr_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = Incr::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incr_parse_non_bulk_string_key() {
    let args = [RespFrame::Integer(123)];
    let err = Incr::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
