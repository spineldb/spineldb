use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::asking::Asking;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_asking_parse_no_args() {
    let args = [];
    let asking_command_result = Asking::parse(&args);
    assert!(asking_command_result.is_ok());
}

#[tokio::test]
async fn test_asking_parse_with_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"extra_arg"))];
    let err = Asking::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_asking_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = Asking::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongArgumentCount(_))
    ));
}
