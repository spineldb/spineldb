use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::lastsave::LastSave;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_lastsave_parse_no_args() {
    let args = [];
    let lastsave_command_result = LastSave::parse(&args);
    assert!(lastsave_command_result.is_ok());
}

#[tokio::test]
async fn test_lastsave_parse_with_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"extra_arg"))];
    let err = LastSave::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_lastsave_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = LastSave::parse(&args);
    assert!(format!("{:?}", result).contains("WrongArgumentCount"));
}
