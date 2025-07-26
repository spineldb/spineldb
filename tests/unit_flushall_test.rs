use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::flushall::FlushAll;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_flushall_parse_no_args() {
    let args = [];
    let flushall_command_result = FlushAll::parse(&args);
    assert!(flushall_command_result.is_ok());
}

#[tokio::test]
async fn test_flushall_parse_with_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"extra_arg"))];
    let err = FlushAll::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_flushall_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = FlushAll::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongArgumentCount(_))
    ));
}
