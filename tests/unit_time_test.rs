use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::time::Time;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_time_parse_no_args() {
    let args = [];
    let time_command_result = Time::parse(&args);
    assert!(time_command_result.is_ok());
}

#[tokio::test]
async fn test_time_parse_too_many_args() {
    let args = [RespFrame::BulkString(Bytes::from("extra_arg"))];
    let err = Time::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_time_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = Time::parse(&args);
    assert!(format!("{:?}", result).contains("WrongArgumentCount"));
}
