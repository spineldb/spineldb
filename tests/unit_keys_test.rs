use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::keys::Keys;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_keys_parse_valid_pattern() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"my*key"))];
    let keys_command = Keys::parse(&args).unwrap();
    assert_eq!(keys_command.pattern, Bytes::from_static(b"my*key"));
}

#[tokio::test]
async fn test_keys_parse_no_args() {
    let args = [];
    let err = Keys::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_keys_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"my*key")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Keys::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_keys_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = Keys::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongType)
    ));
}
