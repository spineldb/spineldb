use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::quit::Quit;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_quit_parse_no_args() {
    let args = [];
    let quit_command_result = Quit::parse(&args);
    assert!(quit_command_result.is_ok());
}

#[tokio::test]
async fn test_quit_parse_with_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"extra_arg"))];
    let err = Quit::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_quit_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = Quit::parse(&args);
    assert!(format!("{:?}", result).contains("WrongArgumentCount"));
}
