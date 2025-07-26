use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::strlen::StrLen;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_strlen_parse_valid() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let strlen_command = StrLen::parse(&args).unwrap();
    assert_eq!(strlen_command.key, Bytes::from_static(b"mykey"));
}

#[tokio::test]
async fn test_strlen_parse_no_args() {
    let args = [];
    let err = StrLen::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_strlen_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = StrLen::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_strlen_parse_non_bulk_string_key() {
    let args = [RespFrame::Integer(123)];
    let err = StrLen::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
