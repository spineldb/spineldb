use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::exists::Exists;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_exists_parse_single_key() {
    let args = [RespFrame::BulkString(Bytes::from("key1"))];
    let exists_command = Exists::parse(&args).unwrap();
    assert_eq!(exists_command.keys, vec![Bytes::from("key1")]);
}

#[tokio::test]
async fn test_exists_parse_multiple_keys() {
    let args = [
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("key2")),
        RespFrame::BulkString(Bytes::from("key3")),
    ];
    let exists_command = Exists::parse(&args).unwrap();
    assert_eq!(
        exists_command.keys,
        vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3")
        ]
    );
}

#[tokio::test]
async fn test_exists_parse_no_args() {
    let args = [];
    let err = Exists::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_exists_parse_non_bulk_string_arg() {
    let args = [RespFrame::SimpleString("key1".to_string())];
    let err = Exists::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
