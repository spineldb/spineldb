use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::echo::Echo;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_echo_parse_with_message() {
    let message = "Hello, SpinelDB!";
    let args = [RespFrame::BulkString(Bytes::from(message))];
    let echo_command = Echo::parse(&args).unwrap();
    assert_eq!(echo_command.message, Bytes::from(message));
}

#[tokio::test]
async fn test_echo_parse_no_args() {
    let args = [];
    let err = Echo::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_echo_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from("Hello")),
        RespFrame::BulkString(Bytes::from("World")),
    ];
    let err = Echo::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_echo_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = Echo::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongType)
    ));
}
