use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::ping::Ping;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_ping_parse_no_args() {
    let args = [];
    let ping_command = Ping::parse(&args).unwrap();
    assert!(ping_command.message.is_none());
}

#[tokio::test]
async fn test_ping_parse_with_message() {
    let message = "Hello, SpinelDB!";
    let args = [RespFrame::BulkString(Bytes::from_static(
        message.as_bytes(),
    ))];
    let ping_command = Ping::parse(&args).unwrap();
    assert_eq!(
        ping_command.message,
        Some(Bytes::from_static(message.as_bytes()))
    );
}

#[tokio::test]
async fn test_ping_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"Hello")),
        RespFrame::BulkString(Bytes::from_static(b"World")),
    ];
    let err = Ping::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_ping_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = Ping::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongType)
    ));
}
