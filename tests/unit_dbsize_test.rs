use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::dbsize::DbSize;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_dbsize_parse_no_args() {
    let args = [];
    let _dbsize_command = DbSize::parse(&args).unwrap();
    // DbSize is a unit struct, so no fields to assert
    assert!(true);
}

#[tokio::test]
async fn test_dbsize_parse_with_args() {
    let args = [RespFrame::BulkString(Bytes::from("arg1"))];
    let err = DbSize::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}
