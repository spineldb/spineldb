use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::flushdb::FlushDb;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_flushdb_parse_no_args() {
    let args = [];
    let flushdb_command_result = FlushDb::parse(&args);
    assert!(flushdb_command_result.is_ok());
}

#[tokio::test]
async fn test_flushdb_parse_with_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"extra_arg"))];
    let err = FlushDb::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_flushdb_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = FlushDb::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongArgumentCount(_))
    ));
}
