use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::list::lpop::LPop;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_lpop_parse_valid() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mylist"))];
    let lpop_command = LPop::parse(&args).unwrap();
    assert_eq!(lpop_command.key, Bytes::from_static(b"mylist"));
}

#[tokio::test]
async fn test_lpop_parse_no_args() {
    let args = [];
    let err = LPop::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_lpop_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = LPop::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_lpop_parse_non_bulk_string_key() {
    let args = [RespFrame::Integer(123)];
    let err = LPop::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
